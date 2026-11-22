using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using DtPipe.Adapters.Shared.Infrastructure.DuckDb;
using DtPipe.Core.Infrastructure.Arrow;
using DuckDB.NET.Native;

namespace DtPipe.Processors.DuckDB;

/// <summary>
/// Registers an Arrow source (--from channel or --ref batch list) as a DuckDB table function
/// backed by <c>duckdb_schema_from_arrow</c> / <c>duckdb_data_chunk_from_arrow</c>, exposing native
/// projection pushdown and deterministic slice release without EXPLAIN inspection or SQL rewriting.
/// </summary>
internal sealed unsafe class DuckDbArrowTableSource : IDisposable
{
    private const int StandardVectorSize = 2048;

    // DuckDB passes COLUMN_IDENTIFIER_ROW_ID (UINT64_MAX) or COLUMN_IDENTIFIER_EMPTY (UINT64_MAX - 1)
    // in duckdb_init_get_column_index for scans that read no columns (e.g. SELECT COUNT(*)).
    private const ulong ColumnIdentifierEmpty = ulong.MaxValue - 1;

    // Keeps exported 1-column RecordBatch slices alive until DuckDB invokes CArrowArray.release.
    private static readonly ConcurrentDictionary<IntPtr, RecordBatch> RetainedExports = new();
    private static IntPtr _exporterRelease;

    private readonly DuckDBNativeConnection _connection;
    private readonly string _alias;
    private readonly bool _isRef;
    private readonly string[] _fieldNames;
    private readonly Schema[] _singleColSchemas;
    private readonly IntPtr[] _convertedColSchemas;
    private readonly DuckDBLogicalType[] _logicalTypes;
    private readonly IReadOnlyList<RecordBatch>? _refBatches;
    private readonly ulong _refTotalRows;
    private readonly ChannelReader<RecordBatch>? _channelReader;
    private readonly CancellationToken _ct;

    private GCHandle _selfHandle;
    private volatile bool _inPrepare;
    private int _activePrepareBinds;
    private int _initCount;
    private bool _disposed;

    private DuckDbArrowTableSource(
        DuckDBNativeConnection connection,
        string alias,
        Schema sourceSchema,
        IReadOnlyList<RecordBatch>? refBatches,
        ChannelReader<RecordBatch>? channelReader,
        CancellationToken ct)
    {
        _connection = connection;
        _alias = alias;
        _isRef = refBatches is not null;
        _refBatches = refBatches;
        _channelReader = channelReader;
        _ct = ct;

        if (refBatches is not null)
        {
            long rows = 0;
            for (var i = 0; i < refBatches.Count; i++) rows += refBatches[i].Length;
            _refTotalRows = (ulong)rows;
        }

        var count = sourceSchema.FieldsList.Count;
        _fieldNames = new string[count];
        _singleColSchemas = new Schema[count];
        _convertedColSchemas = new IntPtr[count];
        _logicalTypes = new DuckDBLogicalType[count];

        try
        {
            // Convert one column at a time: DuckDB v1.5.6's duckdb_data_chunk_from_arrow allocates
            // owned_data inside its column loop while nulling arrow_array->release on column 0, so a
            // multi-column call can release the array before later columns are read.
            for (var i = 0; i < count; i++)
            {
                var field = sourceSchema.FieldsList[i];
                _fieldNames[i] = field.Name;
                var colSchema = new Schema([field], null);
                _singleColSchemas[i] = colSchema;

                _convertedColSchemas[i] = ConvertSingleColumnSchema(connection, colSchema);
                _logicalTypes[i] = ResolveLogicalType(connection, colSchema, _convertedColSchemas[i], field.DataType);
            }

            _selfHandle = GCHandle.Alloc(this);
        }
        catch
        {
            DisposeNativeResources();
            throw;
        }
    }

    public static DuckDbArrowTableSource RegisterRef(
        DuckDBNativeConnection connection,
        string functionName,
        string alias,
        Schema schema,
        IReadOnlyList<RecordBatch> batches)
    {
        var source = new DuckDbArrowTableSource(connection, alias, schema, batches, channelReader: null, CancellationToken.None);
        try
        {
            source.Register(functionName);
            return source;
        }
        catch
        {
            source.Dispose();
            throw;
        }
    }

    public static DuckDbArrowTableSource RegisterStream(
        DuckDBNativeConnection connection,
        string functionName,
        string alias,
        Schema schema,
        ChannelReader<RecordBatch> reader,
        CancellationToken ct)
    {
        var source = new DuckDbArrowTableSource(connection, alias, schema, refBatches: null, reader, ct);
        try
        {
            source.Register(functionName);
            return source;
        }
        catch
        {
            source.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Scopes prepare-time bind counting on a streaming (--from) source so duplicate query references
    /// fail during Prepare without rejecting DuckDB's post-prepare rebinds (e.g. read_parquet).
    /// </summary>
    public void BeginPrepare() => _inPrepare = true;

    public void EndPrepare() => _inPrepare = false;

    private void Register(string functionName)
    {
        var tf = NativeMethods.TableFunction.DuckDBCreateTableFunction();
        try
        {
            NativeMethods.TableFunction.DuckDBTableFunctionSetName(tf, functionName);
            NativeMethods.TableFunction.DuckDBTableFunctionSupportsProjectionPushdown(tf, true);
            NativeMethods.TableFunction.DuckDBTableFunctionSetExtraInfo(tf, GCHandle.ToIntPtr(_selfHandle), null);
            NativeMethods.TableFunction.DuckDBTableFunctionSetBind(tf, &BindCallback);
            NativeMethods.TableFunction.DuckDBTableFunctionSetInit(tf, &InitCallback);
            NativeMethods.TableFunction.DuckDBTableFunctionSetFunction(tf, &ExecCallback);

            if (NativeMethods.TableFunction.DuckDBRegisterTableFunction(_connection, tf) != DuckDBState.Success)
                throw new InvalidOperationException($"Failed to register DuckDB table function '{functionName}' for '{_alias}'.");
        }
        finally
        {
            NativeMethods.TableFunction.DuckDBDestroyTableFunction(ref tf);
        }
    }

    private static IntPtr ConvertSingleColumnSchema(DuckDBNativeConnection connection, Schema singleColSchema)
    {
        CArrowSchema ffiSchema = default;
        CArrowSchemaExporter.ExportSchema(singleColSchema, &ffiSchema);
        try
        {
            var err = DuckDbArrowNative.DuckDBSchemaFromArrow(connection, &ffiSchema, out var converted);
            ThrowIfArrowError(err, "duckdb_schema_from_arrow");
            return converted;
        }
        finally
        {
            // Release exporter's native schema allocations without requiring DuckDB ownership transfer.
            CArrowSchemaImporter.ImportSchema(&ffiSchema);
        }
    }

    private static DuckDBLogicalType ResolveLogicalType(
        DuckDBNativeConnection connection,
        Schema singleColSchema,
        IntPtr convertedSchema,
        IArrowType arrowType)
    {
        using var emptyArray = EmptyArrowArrayFactory.CreateEmpty(arrowType);
        using var emptyBatch = new RecordBatch(singleColSchema, [emptyArray], 0);
        CArrowArray ffiArray = default;
        CArrowArrayExporter.ExportRecordBatch(emptyBatch, &ffiArray);

        var err = DuckDbArrowNative.DuckDBDataChunkFromArrow(connection, &ffiArray, convertedSchema, out var chunk);
        if (err != IntPtr.Zero)
        {
            ReleaseExportedIfUnclaimed(&ffiArray);
            ThrowIfArrowError(err, "duckdb_data_chunk_from_arrow");
        }

        try
        {
            var vector = DuckDbArrowNative.DuckDBDataChunkGetVector(chunk, 0);
            return NativeMethods.Vectors.DuckDBVectorGetColumnType(vector);
        }
        finally
        {
            DuckDbArrowNative.DuckDBDestroyDataChunk(ref chunk);
        }
    }

    private static void ThrowIfArrowError(IntPtr errData, string apiName)
    {
        if (errData == IntPtr.Zero) return;
        var msg = ExtractAndDestroyError(errData);
        throw new InvalidOperationException($"{apiName} failed: {msg}");
    }

    private static string ExtractAndDestroyError(IntPtr errData)
    {
        var msg = Marshal.PtrToStringUTF8(DuckDbArrowNative.DuckDBErrorDataMessage(errData)) ?? "unknown error";
        DuckDbArrowNative.DuckDBDestroyErrorData(ref errData);
        return msg;
    }

    private static string ScannedMoreThanOnceMessage(string alias)
        => $"DuckDBSqlProcessor: the plan scans '{alias}' more than once, and a source is a stream a scan reads to its end: a second scan would find nothing. Read '{alias}' once in a CTE (WITH c AS MATERIALIZED (SELECT * FROM {alias}) ...) and select from that.";

    // ── Unmanaged callbacks (never let managed exceptions unwind across native frames) ──

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void BindCallback(IntPtr info)
    {
        try
        {
            var extraPtr = NativeMethods.TableFunction.DuckDBBindGetExtraInfo(info);
            var source = (DuckDbArrowTableSource)GCHandle.FromIntPtr(extraPtr).Target!;

            if (!source._isRef && source._inPrepare)
            {
                if (Interlocked.Increment(ref source._activePrepareBinds) > 1)
                {
                    Interlocked.Decrement(ref source._activePrepareBinds);
                    NativeMethods.TableFunction.DuckDBBindSetError(info, ScannedMoreThanOnceMessage(source._alias));
                    return;
                }

                // If DuckDB unwinds a failed bind attempt (e.g. extension auto-load in OnPlanningError),
                // ReleasePrepareBind decrements _activePrepareBinds before the retry binds again.
                var bindHandle = GCHandle.Alloc(source);
                NativeMethods.TableFunction.DuckDBBindSetBindData(info, GCHandle.ToIntPtr(bindHandle), &ReleasePrepareBind);
            }

            for (var i = 0; i < source._fieldNames.Length; i++)
                NativeMethods.TableFunction.DuckDBBindAddResultColumn(info, source._fieldNames[i], source._logicalTypes[i]);

            if (source._isRef)
                NativeMethods.TableFunction.DuckDBBindSetCardinality(info, source._refTotalRows, isExact: true);
        }
        catch (Exception ex)
        {
            NativeMethods.TableFunction.DuckDBBindSetError(info, ex.Message);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ReleasePrepareBind(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero) return;
        var handle = GCHandle.FromIntPtr(ptr);
        if (handle.Target is DuckDbArrowTableSource source)
            Interlocked.Decrement(ref source._activePrepareBinds);
        handle.Free();
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void InitCallback(IntPtr info)
    {
        try
        {
            var extraPtr = NativeMethods.TableFunction.DuckDBInitGetExtraInfo(info);
            var source = (DuckDbArrowTableSource)GCHandle.FromIntPtr(extraPtr).Target!;

            if (!source._isRef && Interlocked.Increment(ref source._initCount) > 1)
            {
                NativeMethods.TableFunction.DuckDBInitSetError(info, ScannedMoreThanOnceMessage(source._alias));
                return;
            }

            var colCount = (int)NativeMethods.TableFunction.DuckDBInitGetColumnCount(info);
            var colIds = new ulong[colCount];
            for (var i = 0; i < colCount; i++)
                colIds[i] = NativeMethods.TableFunction.DuckDBInitGetColumnIndex(info, (ulong)i);

            var state = new ScanState(colIds);
            var stateHandle = GCHandle.Alloc(state);
            NativeMethods.TableFunction.DuckDBInitSetInitData(info, GCHandle.ToIntPtr(stateHandle), &DestroyScanState);
        }
        catch (Exception ex)
        {
            NativeMethods.TableFunction.DuckDBInitSetError(info, ex.Message);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void DestroyScanState(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero) return;
        var handle = GCHandle.FromIntPtr(ptr);
        if (handle.Target is ScanState state)
            state.Dispose();
        handle.Free();
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ExecCallback(IntPtr info, IntPtr output)
    {
        try
        {
            var extraPtr = NativeMethods.TableFunction.DuckDBFunctionGetExtraInfo(info);
            var initPtr = NativeMethods.TableFunction.DuckDBFunctionGetInitData(info);
            var source = (DuckDbArrowTableSource)GCHandle.FromIntPtr(extraPtr).Target!;
            var state = (ScanState)GCHandle.FromIntPtr(initPtr).Target!;

            var batch = source.AcquireCurrentBatch(state);
            if (batch is null)
            {
                DuckDbArrowNative.DuckDBDataChunkSetSize(output, 0);
                return;
            }

            var remaining = batch.Length - state.RowOffset;
            var sliceLen = Math.Min(StandardVectorSize, remaining);
            var isWholeBatch = state.RowOffset == 0 && sliceLen == batch.Length;

            using var sliceBatch = isWholeBatch ? null : batch.SliceShared(state.RowOffset, sliceLen);
            var activeBatch = sliceBatch ?? batch;

            try
            {
                var projIds = state.ProjectedColumnIds;
                for (var k = 0; k < projIds.Length; k++)
                {
                    var colId = projIds[k];
                    if (colId >= ColumnIdentifierEmpty || colId >= (ulong)source._fieldNames.Length)
                        continue;

                    var colIdx = (int)colId;
                    var retained = ArrowOwnership.RetainArray(activeBatch.Column(colIdx));
                    var colBatch = new RecordBatch(source._singleColSchemas[colIdx], [retained], sliceLen);

                    if (!TryExportColumnToVector(
                            source._connection,
                            colBatch,
                            source._convertedColSchemas[colIdx],
                            output,
                            (ulong)k,
                            out var errorMsg))
                    {
                        NativeMethods.TableFunction.DuckDBFunctionSetError(info, errorMsg!);
                        return;
                    }
                }
            }
            finally
            {
                state.RowOffset += sliceLen;
                if (state.RowOffset >= batch.Length)
                    state.AdvancePastCurrentBatch(!source._isRef);
            }

            DuckDbArrowNative.DuckDBDataChunkSetSize(output, (ulong)sliceLen);
        }
        catch (OperationCanceledException)
        {
            NativeMethods.TableFunction.DuckDBFunctionSetError(info, "Cancelled");
        }
        catch (Exception ex)
        {
            NativeMethods.TableFunction.DuckDBFunctionSetError(info, ex.Message);
        }
    }

    private RecordBatch? AcquireCurrentBatch(ScanState state)
    {
        if (_isRef)
        {
            while (state.RefBatchIndex < _refBatches!.Count)
            {
                var current = _refBatches[state.RefBatchIndex];
                if (state.RowOffset < current.Length)
                    return current;
                state.RefBatchIndex++;
                state.RowOffset = 0;
            }
            return null;
        }

        while (state.StreamCurrentBatch is null || state.RowOffset >= state.StreamCurrentBatch.Length)
        {
            state.StreamCurrentBatch?.Dispose();
            state.StreamCurrentBatch = null;
            state.RowOffset = 0;

            if (!_channelReader!.TryRead(out var next))
            {
                if (!_channelReader.WaitToReadAsync(_ct).AsTask().GetAwaiter().GetResult())
                    return null;
                if (!_channelReader.TryRead(out next))
                    continue;
            }

            if (next.Length == 0)
            {
                next.Dispose();
                continue;
            }

            state.StreamCurrentBatch = next;
        }

        return state.StreamCurrentBatch;
    }

    private static bool TryExportColumnToVector(
        DuckDBNativeConnection connection,
        RecordBatch colBatch,
        IntPtr convertedColSchema,
        IntPtr outputChunk,
        ulong outputColIndex,
        out string? errorMessage)
    {
        CArrowArray ffiArray = default;
        try
        {
            CArrowArrayExporter.ExportRecordBatch(colBatch, &ffiArray);
        }
        catch
        {
            colBatch.Dispose();
            throw;
        }

        var slot = ReleaseSlot(&ffiArray);
        var innerRelease = *slot;
        var existing = Interlocked.CompareExchange(ref _exporterRelease, innerRelease, IntPtr.Zero);
        if (existing != IntPtr.Zero && existing != innerRelease)
        {
            ((delegate* unmanaged[Cdecl]<CArrowArray*, void>)innerRelease)(&ffiArray);
            colBatch.Dispose();
            throw new InvalidOperationException("Unexpected Arrow exporter release callback mismatch.");
        }

        RetainedExports[(IntPtr)ffiArray.private_data] = colBatch;
        *slot = (IntPtr)(delegate* unmanaged[Cdecl]<CArrowArray*, void>)&ReleaseRetainedBatch;

        var errData = DuckDbArrowNative.DuckDBDataChunkFromArrow(connection, &ffiArray, convertedColSchema, out var tempChunk);
        if (errData != IntPtr.Zero)
        {
            ReleaseExportedIfUnclaimed(&ffiArray);
            errorMessage = ExtractAndDestroyError(errData);
            return false;
        }

        try
        {
            var srcVec = DuckDbArrowNative.DuckDBDataChunkGetVector(tempChunk, 0);
            var dstVec = DuckDbArrowNative.DuckDBDataChunkGetVector(outputChunk, outputColIndex);
            DuckDbArrowNative.DuckDBVectorReferenceVector(dstVec, srcVec);
            errorMessage = null;
            return true;
        }
        finally
        {
            DuckDbArrowNative.DuckDBDestroyDataChunk(ref tempChunk);
        }
    }

    // Apache.Arrow keeps CArrowArray.release internal; the Arrow C Data ABI places it immediately before private_data.
    private static IntPtr* ReleaseSlot(CArrowArray* array) => (IntPtr*)&array->private_data - 1;

    private static void ReleaseExportedIfUnclaimed(CArrowArray* array)
    {
        var releasePtr = *ReleaseSlot(array);
        if (releasePtr != IntPtr.Zero)
            ((delegate* unmanaged[Cdecl]<CArrowArray*, void>)releasePtr)(array);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ReleaseRetainedBatch(CArrowArray* array)
    {
        RetainedExports.TryRemove((IntPtr)array->private_data, out var colBatch);

        var inner = (delegate* unmanaged[Cdecl]<CArrowArray*, void>)_exporterRelease;
        if (inner != null) inner(array);

        colBatch?.Dispose();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_selfHandle.IsAllocated)
            _selfHandle.Free();

        DisposeNativeResources();

        if (_refBatches is not null)
        {
            foreach (var batch in _refBatches)
                batch.Dispose();
        }
    }

    private void DisposeNativeResources()
    {
        for (var i = 0; i < _convertedColSchemas.Length; i++)
        {
            if (_convertedColSchemas[i] != IntPtr.Zero)
                DuckDbArrowNative.DuckDBDestroyArrowConvertedSchema(ref _convertedColSchemas[i]);
        }

        for (var i = 0; i < _logicalTypes.Length; i++)
            _logicalTypes[i]?.Dispose();
    }

    private sealed class ScanState : IDisposable
    {
        public ulong[] ProjectedColumnIds { get; }
        public int RefBatchIndex;
        public RecordBatch? StreamCurrentBatch;
        public int RowOffset;

        public ScanState(ulong[] projectedColumnIds)
        {
            ProjectedColumnIds = projectedColumnIds;
        }

        public void AdvancePastCurrentBatch(bool disposeStreamBatch)
        {
            if (disposeStreamBatch)
            {
                StreamCurrentBatch?.Dispose();
                StreamCurrentBatch = null;
            }
            else
            {
                RefBatchIndex++;
            }
            RowOffset = 0;
        }

        public void Dispose()
        {
            StreamCurrentBatch?.Dispose();
            StreamCurrentBatch = null;
        }
    }
}

/// <summary>
/// Builds 0-row Arrow arrays so DuckDB can resolve <see cref="DuckDBLogicalType"/> during
/// registration without waiting for the first data batch.
/// </summary>
internal static class EmptyArrowArrayFactory
{
    private static readonly ArrowBuffer ZeroInt32Buffer = new ArrowBuffer.Builder<int>(1).Append(0).Build();
    private static readonly ArrowBuffer ZeroInt64Buffer = new ArrowBuffer.Builder<long>(1).Append(0).Build();

    public static IArrowArray CreateEmpty(IArrowType type)
    {
        switch (type)
        {
            case NullType:
                return new NullArray(0);
            case StringType:
                return new StringArray.Builder().Build();
            case BinaryType:
                return new BinaryArray.Builder().Build();
            case LargeStringType:
            case LargeBinaryType:
                return global::Apache.Arrow.ArrowArrayFactory.BuildArray(
                    new ArrayData(type, 0, 0, 0, [ArrowBuffer.Empty, ZeroInt64Buffer, ArrowBuffer.Empty]));
            case StringViewType:
                return new StringViewArray.Builder().Build();
            case BinaryViewType:
                return new BinaryViewArray.Builder().Build();
            case ListType lt:
                return new ListArray(lt, 0, ZeroInt32Buffer, CreateEmpty(lt.ValueDataType), ArrowBuffer.Empty);
            case LargeListType llt:
                return new LargeListArray(llt, 0, ZeroInt64Buffer, CreateEmpty(llt.ValueDataType), ArrowBuffer.Empty);
            case FixedSizeListType fsl:
                return new FixedSizeListArray(fsl, 0, CreateEmpty(fsl.ValueDataType), ArrowBuffer.Empty);
            case ListViewType lvt:
                return global::Apache.Arrow.ArrowArrayFactory.BuildArray(
                    new ArrayData(lvt, 0, 0, 0, [ArrowBuffer.Empty, ArrowBuffer.Empty, ArrowBuffer.Empty], [CreateEmpty(lvt.ValueDataType).Data]));
            case LargeListViewType llvt:
                return global::Apache.Arrow.ArrowArrayFactory.BuildArray(
                    new ArrayData(llvt, 0, 0, 0, [ArrowBuffer.Empty, ArrowBuffer.Empty, ArrowBuffer.Empty], [CreateEmpty(llvt.ValueDataType).Data]));
            case StructType st:
                return new StructArray(st, 0, st.Fields.Select(f => CreateEmpty(f.DataType)).ToArray(), ArrowBuffer.Empty);
            case MapType mt:
                return new MapArray(mt, 0, ZeroInt32Buffer, CreateEmpty(mt.Fields[0].DataType), ArrowBuffer.Empty);
            case DictionaryType dt:
                return new DictionaryArray(dt, CreateEmpty(dt.IndexType), CreateEmpty(dt.ValueType));
            case RunEndEncodedType ree:
                return new RunEndEncodedArray(CreateEmpty(ree.RunEndsDataType), CreateEmpty(ree.ValuesDataType));
            case UnionType ut:
                return global::Apache.Arrow.ArrowArrayFactory.BuildArray(
                    new ArrayData(
                        ut, 0, 0, 0,
                        ut.Mode == UnionMode.Dense ? [ArrowBuffer.Empty, ArrowBuffer.Empty] : [ArrowBuffer.Empty],
                        ut.Fields.Select(f => CreateEmpty(f.DataType).Data).ToArray()));
            default:
                return global::Apache.Arrow.ArrowArrayFactory.BuildArray(
                    new ArrayData(type, 0, 0, 0, [ArrowBuffer.Empty, ArrowBuffer.Empty]));
        }
    }
}
