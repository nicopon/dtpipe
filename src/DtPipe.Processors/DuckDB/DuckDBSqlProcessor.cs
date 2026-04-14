using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.C;
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

    // Input-side: CArrowArrayStream structs allocated on the unmanaged heap for DuckDB to hold.
    private readonly List<IntPtr> _allocatedPointers = new();

    private DuckDBConnection? _conn;
    private DuckDBPreparedStatement? _stmt;
    private IntPtr _arrowOpts = IntPtr.Zero;
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

            using (var cmd = _conn.CreateCommand())
            {
                // Export Arrow extension types faithfully (UUID → FixedSizeBinary(16)+arrow.uuid
                // instead of the default lossy Utf8). Requires DuckDB >= v1.2.0; bundled v1.5.0.
                cmd.CommandText = "SET arrow_lossless_conversion = true";
                await cmd.ExecuteNonQueryAsync(ct);
            }

            using (var cmd = _conn.CreateCommand())
            {
                // duckdb_arrow_scan (used to register CDI streaming sources) declares filter_pushdown=true,
                // which causes DuckDB's optimizer to remove Filter operators from the plan trusting that
                // the scan will apply them. But the C API wrapper (FactoryGetNext) ignores ArrowStreamParameters
                // entirely — the filter is never applied and all rows are returned regardless of WHERE clauses.
                // Disabling filter_pushdown forces DuckDB to keep Filter operators in the plan where they
                // are correctly executed after the scan. This is the right behaviour for opaque CDI streams.
                cmd.CommandText = "SET disabled_optimizers='filter_pushdown'";
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

            _logger.LogDebug("DuckDBSqlProcessor: Sources registered. Preparing statement...");
            PrepareStatement();

            // Arrow options carry the arrow_lossless_conversion flag and are used for both
            // schema construction (duckdb_to_arrow_schema) and chunk conversion (duckdb_data_chunk_to_arrow).
            DuckDBArrowNativeMethods.DuckDBConnectionGetArrowOptions(_conn.NativeConnection, out _arrowOpts);

            _logger.LogDebug("DuckDBSqlProcessor: Inspecting schema from prepared statement...");
            _resultSchema = InspectSchemaFromStatement();
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

    // ── Input registration ───────────────────────────────────────────────────────────

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
            batches.Add(batch);

        var stream = new StaticArrowStream(schema, batches);
        RegisterArrowStream(alias, stream);
    }

    // Registers the main source as a streaming Arrow scan in DuckDB (zero-copy).
    private async Task RegisterStreamingTableAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        ValidateSchema(channelAlias, schema);
        var channelTuple = _registry.GetArrowChannel(channelAlias)
            ?? throw new Exception($"Arrow channel '{channelAlias}' not found");

        var stream = new ChannelArrowStream(schema, channelTuple.Channel.Reader, _logger, ct);
        RegisterArrowStream(alias, stream);
    }

    private void RegisterArrowStream(string alias, IArrowArrayStream stream)
    {
        unsafe
        {
            var ffiStreamPtr = (CArrowArrayStream*)Marshal.AllocHGlobal(Marshal.SizeOf<CArrowArrayStream>());
            _allocatedPointers.Add((IntPtr)ffiStreamPtr);
            CArrowArrayStreamExporter.ExportArrayStream(stream, ffiStreamPtr);

            if (DuckDBArrowNativeMethods.DuckDBArrowScan(_conn!.NativeConnection, alias, ffiStreamPtr) != DuckDBState.Success)
            {
                CArrowArrayStreamImporter.ImportArrayStream(ffiStreamPtr).Dispose();
                _allocatedPointers.Remove((IntPtr)ffiStreamPtr);
                Marshal.FreeHGlobal((IntPtr)ffiStreamPtr);
                throw new Exception($"Failed to register Arrow stream scan for '{alias}'");
            }
        }
    }

    // ── Schema inspection from prepared statement ────────────────────────────────────

    // Prepares the query statement. The prepared statement is reused across
    // OpenAsync (schema) and ReadRecordBatchesAsync (streaming execution).
    private void PrepareStatement()
    {
        if (DuckDBArrowNativeMethods.DuckDBPrepare(_conn!.NativeConnection, _query, out var stmt) != DuckDBState.Success)
        {
            stmt.Dispose();
            throw new Exception($"DuckDB prepare failed for query: {_query}");
        }
        _stmt = stmt;
    }

    // Builds the Arrow schema directly from the prepared statement — no LIMIT 0 probe query.
    // Uses duckdb_prepared_statement_column_* to get DuckDB logical types, then
    // duckdb_to_arrow_schema (with arrow_lossless_conversion = true) to produce the Arrow schema.
    private unsafe Schema InspectSchemaFromStatement()
    {
        var count = DuckDBArrowNativeMethods.DuckDBPreparedStatementColumnCount(_stmt!);
        if (count == 0)
            throw new Exception($"Prepared statement returned 0 columns for query: {_query}");

        var logicalTypes = new DuckDBLogicalType[count];
        var namePointers = new IntPtr[count];

        try
        {
            for (ulong i = 0; i < count; i++)
            {
                logicalTypes[i] = DuckDBArrowNativeMethods.DuckDBPreparedStatementColumnLogicalType(_stmt!, i);
                namePointers[i] = DuckDBArrowNativeMethods.DuckDBPreparedStatementColumnName(_stmt!, i);
            }

            // Build raw pointer arrays for the C API call.
            // DangerousGetHandle() is safe here: handles outlive the fixed block.
            var rawTypes = System.Array.ConvertAll(logicalTypes, t => t.DangerousGetHandle());

            CArrowSchema ffiSchema = default;
            fixed (IntPtr* pTypes = rawTypes)
            fixed (IntPtr* pNames = namePointers)
            {
                var errData = DuckDBArrowNativeMethods.DuckDBToArrowSchema(
                    _arrowOpts, pTypes, (byte**)pNames, count, &ffiSchema);

                if (errData != IntPtr.Zero)
                {
                    var msg = Marshal.PtrToStringUTF8(DuckDBArrowNativeMethods.DuckDBErrorMessage(errData))
                        ?? "unknown error";
                    DuckDBArrowNativeMethods.DuckDBDestroyErrorData(ref errData);
                    throw new Exception($"duckdb_to_arrow_schema failed: {msg}");
                }
            }

            return CArrowSchemaImporter.ImportSchema(&ffiSchema);
        }
        finally
        {
            foreach (var namePtr in namePointers)
                if (namePtr != IntPtr.Zero) DuckDBArrowNativeMethods.DuckDBFree(namePtr);

            foreach (var lt in logicalTypes)
                lt?.Dispose();
        }
    }

    // ── Streaming output ─────────────────────────────────────────────────────────────

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_conn is null || _stmt is null) yield break;

        var result = ExecuteStreamingQuery();
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // FetchAndConvertChunk is extracted as a non-async unsafe method because
                // async iterators cannot take the address of local variables (&ffiArray).
                var batch = FetchAndConvertChunk(result);
                if (batch == null) yield break;
                yield return batch;
            }
        }
        finally
        {
            DuckDBArrowNativeMethods.DuckDBDestroyResult(ref result);
        }
    }

    // Executes the prepared statement as a lazy streaming result.
    // duckdb_execute_prepared_streaming is deprecated (scheduled for removal) but is the
    // only C API path that avoids full result materialisation. duckdb_fetch_chunk (used
    // in FetchAndConvertChunk) and duckdb_data_chunk_to_arrow are both non-deprecated.
    private DuckDBResult ExecuteStreamingQuery()
    {
        if (DuckDBArrowNativeMethods.DuckDBExecutePreparedStreaming(_stmt!, out var result) != DuckDBState.Success)
        {
            var errPtr = DuckDBArrowNativeMethods.DuckDBResultError(ref result);
            var msg = errPtr != IntPtr.Zero ? Marshal.PtrToStringUTF8(errPtr) : "unknown error";
            DuckDBArrowNativeMethods.DuckDBDestroyResult(ref result);
            throw new Exception($"DuckDB streaming execute failed: {msg}");
        }

        if (!DuckDBArrowNativeMethods.DuckDBResultIsStreaming(result))
            _logger.LogDebug(
                "DuckDBSqlProcessor: optimizer chose materialized execution (non-streaming). " +
                "Result is correct but fully buffered in DuckDB memory before first batch.");

        return result;
    }

    // Fetches the next chunk from the streaming result and converts it to a RecordBatch.
    // Extracted from ReadRecordBatchesAsync to allow taking addresses of local structs
    // (not permitted in async methods). Returns null when the stream is exhausted.
    // duckdb_data_chunk_to_arrow copies buffer data — the chunk can be disposed immediately.
    private unsafe RecordBatch? FetchAndConvertChunk(DuckDBResult result)
    {
        using var chunk = DuckDBArrowNativeMethods.DuckDBFetchChunk(result);
        if (chunk.IsInvalid) return null;

        CArrowArray ffiArray = default;
        var errData = DuckDBArrowNativeMethods.DuckDBDataChunkToArrow(_arrowOpts, chunk, &ffiArray);
        if (errData != IntPtr.Zero)
        {
            var msg = Marshal.PtrToStringUTF8(DuckDBArrowNativeMethods.DuckDBErrorMessage(errData))
                ?? "unknown error";
            DuckDBArrowNativeMethods.DuckDBDestroyErrorData(ref errData);
            throw new Exception($"duckdb_data_chunk_to_arrow failed: {msg}");
        }

        return CArrowArrayImporter.ImportRecordBatch(&ffiArray, _resultSchema!);
    }

    // ── Row-mode fallback ─────────────────────────────────────────────────────────────

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var recordBatch in ReadRecordBatchesAsync(ct))
        {
            using (recordBatch)
                yield return ConvertBatchToRows(recordBatch);
        }
    }

    // ── Disposal ──────────────────────────────────────────────────────────────────────

    public void Dispose()
    {
        if (_arrowOpts != IntPtr.Zero) { DuckDBArrowNativeMethods.DuckDBDestroyArrowOptions(ref _arrowOpts); }
        _stmt?.Dispose();
        _stmt = null;

        foreach (var ptr in _allocatedPointers)
            Marshal.FreeHGlobal(ptr);
        _allocatedPointers.Clear();

        _conn?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_arrowOpts != IntPtr.Zero) { DuckDBArrowNativeMethods.DuckDBDestroyArrowOptions(ref _arrowOpts); }
        _stmt?.Dispose();
        _stmt = null;

        foreach (var ptr in _allocatedPointers)
            Marshal.FreeHGlobal(ptr);
        _allocatedPointers.Clear();

        if (_conn is not null) { await _conn.DisposeAsync(); _conn = null; }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────────

    private static ReadOnlyMemory<object?[]> ConvertBatchToRows(RecordBatch batch)
        => SqlProcessorHelpers.ConvertBatchToRows(batch);

    private void ValidateAliases()
        => SqlProcessorHelpers.ValidateAliases(_mainChannelAlias, _refChannelAliases);

    private static void ValidateSchema(string alias, Schema schema)
        => SqlProcessorHelpers.ValidateSchema(alias, schema);
}
