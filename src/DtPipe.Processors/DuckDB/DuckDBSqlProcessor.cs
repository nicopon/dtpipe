using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Expressions;
using DtPipe.Core.Security;
using DtPipe.Adapters.Shared.Infrastructure.DuckDb;
using DtPipe.Processors.Sql;
using DuckDB.NET.Data;
using DuckDB.NET.Native;
using Microsoft.Extensions.Logging;

namespace DtPipe.Processors.DuckDB;

public sealed class DuckDBSqlProcessor : IColumnarStreamReader, IDisposable
{
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _query;
    private readonly string? _initSql;
    private readonly IStringContentResolver? _resolver;
    private readonly string _mainAlias;
    private readonly string _mainChannelAlias;
    private readonly string[] _refAliases;
    private readonly string[] _refChannelAliases;
    private readonly ILogger<DuckDBSqlProcessor> _logger;

    // Input-side: CArrowArrayStream structs allocated on the unmanaged heap for DuckDB to hold.
    private readonly List<IArrowArrayStream> _activeStreams = new();
    private readonly List<IntPtr> _allocatedPointers = new();
    private readonly Dictionary<string, IProjectableArrowStream> _streamProjections = new();

    private DuckDBConnection? _conn;
    private DuckDbArrowResultReader? _resultReader;
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
        ILogger<DuckDBSqlProcessor> logger,
        string? initSql = null,
        IStringContentResolver? resolver = null)
    {
        _registry = registry;
        _query = query;
        _initSql = initSql;
        _resolver = resolver;
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
                // duckdb_arrow_scan (used to register CDI streaming sources) declares filter_pushdown=true,
                // which causes DuckDB's optimizer to remove Filter operators from the plan trusting that
                // the scan will apply them. But the C API wrapper (FactoryGetNext) ignores ArrowStreamParameters
                // entirely — the filter is never applied and all rows are returned regardless of WHERE clauses.
                // Disabling filter_pushdown forces DuckDB to keep Filter operators in the plan where they
                // are correctly executed after the scan. This is the right behaviour for opaque CDI streams.
                cmd.CommandText = "SET disabled_optimizers='filter_pushdown'";
                await cmd.ExecuteNonQueryAsync(ct);

                // Export Arrow extension types faithfully (UUID -> FixedSizeBinary(16)+arrow.uuid)
                cmd.CommandText = "SET arrow_lossless_conversion = true";
                await cmd.ExecuteNonQueryAsync(ct);
            }

            await DuckInitSqlRunner.RunAsync(_conn, _initSql, _resolver, ct);

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
            await ApplyProjectionsFromExplainAsync(ct);

            _logger.LogDebug("DuckDBSqlProcessor: Inspecting schema from prepared statement...");
            _resultReader = new DuckDbArrowResultReader(_conn, _query, _logger);
            _resultSchema = _resultReader.Prepare();

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
        await RegisterArrowStreamAsync(alias, stream, ct);
    }

    // Registers the main source as a streaming Arrow scan in DuckDB (zero-copy).
    private async Task RegisterStreamingTableAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        ValidateSchema(channelAlias, schema);
        var channelTuple = _registry.GetArrowChannel(channelAlias)
            ?? throw new Exception($"Arrow channel '{channelAlias}' not found");

        var stream = new ChannelArrowStream(schema, channelTuple.Channel.Reader, _logger, ct);
        await RegisterArrowStreamAsync(alias, stream, ct);
    }

    private static string? GetDuckDBCastForExtension(string extensionName)
    {
        return extensionName switch
        {
            "arrow.uuid" => "UUID",
            "arrow.json" => "JSON",
            "arrow.bool8" => "BOOLEAN",
            _ => null
        };
    }

    private async Task RegisterArrowStreamAsync(string alias, IArrowArrayStream underlyingStream, CancellationToken ct)
    {
        var stream = new ProjectedArrowStream(underlyingStream);
        _streamProjections[alias] = stream;

        var schema = stream.Schema;
        var needsView = false;
        foreach (var f in schema.FieldsList)
        {
            if (f.HasMetadata && f.Metadata.TryGetValue("ARROW:extension:name", out var ext))
            {
                if (GetDuckDBCastForExtension(ext) != null)
                {
                    needsView = true;
                }
                else if (ext != "arrow.opaque" && ext != "arrow.parquet.variant")
                {
                    _logger.LogWarning("DuckDBSqlProcessor: Unhandled canonical Arrow extension '{Extension}' on field '{Field}'. Falling back to physical type.", ext, f.Name);
                }
            }
        }

        string scanAlias = needsView ? $"{alias}__raw_cdi" : alias;

        unsafe
        {
            _activeStreams.Add(stream);
            var ffiStreamPtr = (CArrowArrayStream*)Marshal.AllocHGlobal(Marshal.SizeOf<CArrowArrayStream>());
            _allocatedPointers.Add((IntPtr)ffiStreamPtr);
            CArrowArrayStreamExporter.ExportArrayStream(stream, ffiStreamPtr);

            if (DuckDbArrowNative.DuckDBArrowScan(_conn!.NativeConnection, scanAlias, ffiStreamPtr) != DuckDBState.Success)
            {
                _activeStreams.Remove(stream);
                CArrowArrayStreamImporter.ImportArrayStream(ffiStreamPtr).Dispose();
                _allocatedPointers.Remove((IntPtr)ffiStreamPtr);
                Marshal.FreeHGlobal((IntPtr)ffiStreamPtr);
                throw new Exception($"Failed to register Arrow stream scan for '{alias}'");
            }
        }

        if (needsView)
        {
            var columns = schema.FieldsList.Select(f => {
                if (f.HasMetadata && f.Metadata.TryGetValue("ARROW:extension:name", out var ext))
                {
                    var castType = GetDuckDBCastForExtension(ext);
                    if (castType != null)
                        return $"CAST(\"{f.Name}\" AS {castType}) AS \"{f.Name}\"";
                }
                return $"\"{f.Name}\"";
            });
            var viewSql = $"CREATE VIEW \"{alias}\" AS SELECT {string.Join(", ", columns)} FROM \"{scanAlias}\"";

            _logger.LogDebug("DuckDBSqlProcessor: Restoring Arrow extension semantics via semantic view '{Alias}'", alias);
            using var cmd = _conn!.CreateCommand();
            cmd.CommandText = viewSql;
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    // ── Schema inspection from prepared statement ────────────────────────────────────

    private async Task ApplyProjectionsFromExplainAsync(CancellationToken ct)
    {
        try
        {
            using var cmd = _conn!.CreateCommand();
            cmd.CommandText = $"EXPLAIN (FORMAT JSON) {_query}";

            using var reader = await cmd.ExecuteReaderAsync(ct);

            // Parse projections as an ORDERED list. The EXPLAIN JSON lists ARROW_SCAN projections
            // in the same order as DuckDB's internal column_ids vector, which determines the
            // expected children[] position in each CDI batch (arrow_scan_is_projected = true
            // hardcoded). Losing this order (e.g. by using a HashSet) would map children[idx]
            // to the wrong column for any multi-column projection where query order ≠ schema order.
            var projectedColumnsOrdered = new List<string>();

            while (await reader.ReadAsync(ct))
            {
                if (reader.FieldCount < 2 || reader.IsDBNull(1)) continue;

                var json = reader.GetValue(1)?.ToString();
                if (string.IsNullOrEmpty(json)) continue;

                ParseProjectionsFromJson(json, projectedColumnsOrdered);
            }

            foreach (var kvp in _streamProjections)
            {
                var alias = kvp.Key;
                var stream = kvp.Value;

                if (stream?.Schema?.FieldsList == null) continue;

                // Build a fast-lookup set of this stream's column names for the membership test,
                // but iterate the EXPLAIN-ordered list to build the final projection — preserving
                // the order DuckDB expects in the CDI batch.
                var streamColSet = new HashSet<string>(
                    stream.Schema.FieldsList.Select(f => f.Name),
                    StringComparer.OrdinalIgnoreCase);

                var projectedForStream = new List<string>();
                foreach (var c in projectedColumnsOrdered)
                {
                    if (streamColSet.Contains(c))
                    {
                        if (!projectedForStream.Contains(c)) projectedForStream.Add(c);
                    }
                    else
                    {
                        // Handle DuckDB pushing down nested struct projections (e.g., "ComplexObject._value")
                        // by projecting the root column ("ComplexObject") to pass the full struct.
                        var dotIndex = c.IndexOf('.');
                        if (dotIndex > 0)
                        {
                            var rootCol = c.Substring(0, dotIndex);
                            if (streamColSet.Contains(rootCol))
                            {
                                if (!projectedForStream.Contains(rootCol)) projectedForStream.Add(rootCol);
                            }
                        }
                    }
                }

                if (projectedForStream.Count > 0)
                {
                    _logger.LogDebug("DuckDBSqlProcessor: Setting projection for '{Alias}': [{Cols}]", alias, string.Join(", ", projectedForStream));
                    stream.SetProjectedColumns(projectedForStream);
                }
            }
        }
        catch (Exception ex)
        {
            throw new Exception(
                "DuckDBSqlProcessor: Pre-flight EXPLAIN failed. Aborting query to prevent potential data corruption or native crashes due to DuckDB projection pushdown bugs.", ex);
        }
    }

    // Parses ARROW_SCAN projection lists from a DuckDB EXPLAIN (FORMAT JSON) result.
    // Projections are appended to `ordered` in the order they appear in the JSON, which
    // matches DuckDB's column_ids order and therefore the expected CDI children[] position.
    private void ParseProjectionsFromJson(string json, List<string> ordered)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Array)
                foreach (var item in root.EnumerateArray())
                    TraversePlanForProjections(item, ordered);
            else if (root.ValueKind == JsonValueKind.Object)
                TraversePlanForProjections(root, ordered);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DuckDBSqlProcessor: Failed to parse EXPLAIN JSON");
        }
    }

    private static void TraversePlanForProjections(JsonElement element, List<string> ordered)
    {
        if (element.TryGetProperty("name", out var nameProp) &&
            nameProp.GetString() == "ARROW_SCAN" &&
            element.TryGetProperty("extra_info", out var extraInfo) &&
            extraInfo.TryGetProperty("Projections", out var projProp))
        {
            if (projProp.ValueKind == JsonValueKind.Array)
            {
                foreach (var p in projProp.EnumerateArray())
                {
                    var col = p.GetString();
                    if (!string.IsNullOrEmpty(col) &&
                        !ordered.Contains(col, StringComparer.OrdinalIgnoreCase))
                        ordered.Add(col);
                }
            }
            else if (projProp.ValueKind == JsonValueKind.String)
            {
                var col = projProp.GetString();
                if (!string.IsNullOrEmpty(col) &&
                    !ordered.Contains(col, StringComparer.OrdinalIgnoreCase))
                    ordered.Add(col);
            }
        }

        if (element.TryGetProperty("children", out var children) &&
            children.ValueKind == JsonValueKind.Array)
            foreach (var child in children.EnumerateArray())
                TraversePlanForProjections(child, ordered);
    }

    // ── Streaming output ─────────────────────────────────────────────────────────────

    public IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(CancellationToken ct = default)
        => _resultReader?.ReadRecordBatchesAsync(ct) ?? EmptyBatches();

    private static async IAsyncEnumerable<RecordBatch> EmptyBatches()
    {
        await Task.CompletedTask;
        yield break;
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
        _resultReader?.Dispose();
        _resultReader = null;

        foreach (var ptr in _allocatedPointers)
            Marshal.FreeHGlobal(ptr);

        foreach (var stream in _activeStreams)
            stream.Dispose();

        _allocatedPointers.Clear();
        _activeStreams.Clear();

        _conn?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        _resultReader?.Dispose();
        _resultReader = null;

        foreach (var ptr in _allocatedPointers)
            Marshal.FreeHGlobal(ptr);

        foreach (var stream in _activeStreams)
            stream.Dispose();

        _allocatedPointers.Clear();
        _activeStreams.Clear();

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
