using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Processors.Sql;
using DuckDB.NET.Data;
using DuckDB.NET.Native;
using Microsoft.Extensions.Logging;
using System.Runtime.InteropServices;

namespace DtPipe.Processors.DuckDB;

public sealed class DuckDBSqlProcessor : IColumnarStreamReader, IDisposable
{
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _query;
    private readonly string _mainAlias;
    private readonly string _mainChannelAlias;
    private readonly string[] _refAliases;
    private readonly string[] _refChannelAliases;
    private readonly ILogger<DuckDBSqlProcessor> _logger;

    private readonly List<IntPtr> _allocatedPointers = new();

    private DuckDBConnection? _conn;
    private Schema? _resultSchema;
    private IReadOnlyList<PipeColumnInfo>? _columns;

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;
    public Schema? Schema => _resultSchema;

    public DuckDBSqlProcessor(
        IMemoryChannelRegistry registry,
        string query,
        string mainAlias,
        string mainChannelAlias,
        string[] refAliases,
        string[] refChannelAliases,
        ILogger<DuckDBSqlProcessor> logger)
    {
        _registry = registry;
        _query = query;
        _mainAlias = mainAlias;
        _mainChannelAlias = mainChannelAlias;
        _refAliases = refAliases;
        _refChannelAliases = refChannelAliases;
        _logger = logger;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        _logger.LogDebug("DuckDBSqlProcessor: OpenAsync — query={Query}", _query);
        try
        {
            _conn = new DuckDBConnection("DataSource=:memory:");
            await _conn.OpenAsync(ct);

            // Export Arrow extension types faithfully (e.g. UUID → FixedSizeBinary(16)+arrow.uuid
            // instead of the default lossy Utf8). Requires DuckDB >= v1.2.0; bundled v1.5.0.
            using (var cmd = _conn.CreateCommand())
            {
                cmd.CommandText = "SET arrow_lossless_conversion = true";
                await cmd.ExecuteNonQueryAsync(ct);
            }

            ValidateAliases();

            if (_refAliases.Length > 0)
            {
                var tasks = _refAliases
                    .Select((alias, i) => RegisterRefTableAsync(alias, _refChannelAliases[i], ct))
                    .ToList();
                await Task.WhenAll(tasks);
            }

            if (!string.IsNullOrEmpty(_mainAlias))
                await RegisterStreamingTableAsync(_mainAlias, _mainChannelAlias, ct);

            _logger.LogDebug("DuckDBSqlProcessor: All sources registered. Inspecting schema...");
            _resultSchema = InspectSchemaViaArrow();
            _columns = _resultSchema.FieldsList
                .Select(f => new PipeColumnInfo(f.Name, ArrowTypeMapper.GetClrTypeFromField(f), f.IsNullable))
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DuckDBSqlProcessor: OpenAsync FAILED: {Message}", ex.Message);
            throw;
        }
    }

    // Materialises a --ref source fully into an in-memory list of RecordBatches,
    // then registers it as a zero-copy Arrow scan in DuckDB.
    // Draining is required because ref tables are often joined multiple times.
    private async Task RegisterRefTableAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        ValidateSchema(channelAlias, schema);
        var channelTuple = _registry.GetArrowChannel(channelAlias)
            ?? throw new Exception($"Arrow channel '{channelAlias}' not found");

        var batches = new List<RecordBatch>();
        await foreach (var batch in channelTuple.Channel.Reader.ReadAllAsync(ct))
        {
            batches.Add(batch);
        }

        var stream = new StaticArrowStream(schema, batches);
        await RegisterArrowStreamAsync(alias, stream);
    }

    // Registers the source as an Arrow stream scan in DuckDB.
    // This allows true zero-copy data transfer from the memory channel to DuckDB.
    private async Task RegisterStreamingTableAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        ValidateSchema(channelAlias, schema);
        var channelTuple = _registry.GetArrowChannel(channelAlias)
            ?? throw new Exception($"Arrow channel '{channelAlias}' not found");

        var stream = new ChannelArrowStream(schema, channelTuple.Channel.Reader, _logger, ct);
        await RegisterArrowStreamAsync(alias, stream);
    }

    private Task RegisterArrowStreamAsync(string alias, IArrowArrayStream stream)
    {
        unsafe
        {
            var ffiStreamPtr = (Apache.Arrow.C.CArrowArrayStream*)Marshal.AllocHGlobal(Marshal.SizeOf<Apache.Arrow.C.CArrowArrayStream>());
            _allocatedPointers.Add((IntPtr)ffiStreamPtr);
            Apache.Arrow.C.CArrowArrayStreamExporter.ExportArrayStream(stream, ffiStreamPtr);

            if (DuckDBArrowNativeMethods.DuckDBArrowScan(_conn!.NativeConnection, alias, ffiStreamPtr) != DuckDBState.Success)
            {
                Apache.Arrow.C.CArrowArrayStreamImporter.ImportArrayStream(ffiStreamPtr).Dispose();
                _allocatedPointers.Remove((IntPtr)ffiStreamPtr);
                Marshal.FreeHGlobal((IntPtr)ffiStreamPtr);
                throw new Exception($"Failed to register Arrow stream scan for '{alias}'");
            }
        }
        return Task.CompletedTask;
    }

    // Uses the same Arrow C Data Interface path as ReadRecordBatchesAsync so that _resultSchema
    // is always consistent with the actual RecordBatch schema DuckDB produces.
    // Note: DuckDB exports UUID columns as Utf8 (StringType), not FixedSizeBinary+arrow.uuid.
    private unsafe Schema InspectSchemaViaArrow()
    {
        var probeQuery = $"SELECT * FROM ({_query}) __schema_probe LIMIT 0";
        if (DuckDBArrowNativeMethods.DuckDBQueryArrow(_conn!.NativeConnection, probeQuery, out var arrowResult) != DuckDBState.Success)
            throw new Exception($"DuckDB schema probe failed for query: {_query}");
        try
        {
            return GetSchemaFromResult(arrowResult)
                ?? throw new Exception("DuckDB returned an empty schema for the query.");
        }
        finally
        {
            nint ptr = arrowResult.InternalPtr;
            DuckDBArrowNativeMethods.DuckDBDestroyArrow(ref ptr);
        }
    }

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_conn is null) yield break;

        DuckDBArrowNativeMethods.DuckDBArrow arrowResult;
        unsafe 
        {
            if (DuckDBArrowNativeMethods.DuckDBQueryArrow(_conn.NativeConnection, _query, out arrowResult) != DuckDBState.Success)
            {
                throw new Exception("DuckDB query_arrow execution failed.");
            }
        }

        try
        {
            await foreach (var batch in ReadRecordBatchesFromArrowResultAsync(arrowResult, ct))
                yield return batch;
        }
        finally
        {
            unsafe 
            {
                nint ptr = arrowResult.InternalPtr;
                DuckDBArrowNativeMethods.DuckDBDestroyArrow(ref ptr);
            }
        }
    }

    private async IAsyncEnumerable<RecordBatch> ReadRecordBatchesFromArrowResultAsync(
        DuckDBArrowNativeMethods.DuckDBArrow result, [EnumeratorCancellation] CancellationToken ct)
    {
        Schema? arrowSchema = GetSchemaFromResult(result);
        if (arrowSchema == null) yield break;

        while (true)
        {
            RecordBatch? batch = FetchNextBatch(result, arrowSchema);
            if (batch == null) break;
            yield return batch;
        }
    }

    private unsafe Schema? GetSchemaFromResult(DuckDBArrowNativeMethods.DuckDBArrow result)
    {
        Apache.Arrow.C.CArrowSchema ffiSchema = default;
        Apache.Arrow.C.CArrowSchema* pSchema = &ffiSchema;
        // DuckDB fills the struct pointed to by pSchema
        if (DuckDBArrowNativeMethods.DuckDBQueryArrowSchema(result, (nint*)&pSchema) == DuckDBState.Success)
        {
            // The importer will throw if release is null, so we just try to import.
            try
            {
                return Apache.Arrow.C.CArrowSchemaImporter.ImportSchema(&ffiSchema);
            }
            catch (ArgumentException)
            {
                return null;
            }
        }
        return null;
    }

    private unsafe RecordBatch? FetchNextBatch(DuckDBArrowNativeMethods.DuckDBArrow result, Schema schema)
    {
        Apache.Arrow.C.CArrowArray ffiArray = default;
        Apache.Arrow.C.CArrowArray* pArray = &ffiArray;

        if (DuckDBArrowNativeMethods.DuckDBQueryArrowArray(result, (nint*)&pArray) != DuckDBState.Success) return null;

        // Note: Field 'release' is internal, but the importer checks it.
        try
        {
            return Apache.Arrow.C.CArrowArrayImporter.ImportRecordBatch(&ffiArray, schema);
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    public void Dispose()
    {
        foreach (var ptr in _allocatedPointers)
        {
            Marshal.FreeHGlobal(ptr);
        }
        _allocatedPointers.Clear();

        _conn?.Dispose();
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var recordBatch in ReadRecordBatchesAsync(ct))
        {
            using (recordBatch)
                yield return ConvertBatchToRows(recordBatch);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var ptr in _allocatedPointers)
            Marshal.FreeHGlobal(ptr);
        _allocatedPointers.Clear();
        if (_conn is not null) { await _conn.DisposeAsync(); _conn = null; }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static ReadOnlyMemory<object?[]> ConvertBatchToRows(RecordBatch batch)
        => SqlProcessorHelpers.ConvertBatchToRows(batch);

    private void ValidateAliases()
        => SqlProcessorHelpers.ValidateAliases(_mainChannelAlias, _refChannelAliases);

    private static void ValidateSchema(string alias, Schema schema)
        => SqlProcessorHelpers.ValidateSchema(alias, schema);
}
