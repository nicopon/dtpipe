using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.RegularExpressions;
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

public sealed class DuckDBSqlProcessor : IColumnarStreamReader, ISqlBearing, IDisposable
{
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _query;
    // The text DuckDB plans and runs: the query, with a CTE in front when a --ref must be read once.
    private string _statement;
    private readonly string? _initSql;
    private readonly IStringContentResolver? _resolver;
    private readonly string _mainAlias;
    private readonly string _mainChannelAlias;
    private readonly string[] _refAliases;
    private readonly string[] _refChannelAliases;
    private readonly ILogger<DuckDBSqlProcessor> _logger;
    private readonly bool _edgesOnly;

    // Input-side: CArrowArrayStream structs allocated on the unmanaged heap for DuckDB to hold.
    private readonly List<IArrowArrayStream> _activeStreams = new();
    private readonly List<IntPtr> _allocatedPointers = new();
    private readonly Dictionary<string, IProjectableArrowStream> _streamProjections = new();
    private readonly Dictionary<string, string> _viewBodies = new();
    private int _streamOrdinal;

    private DuckDBConnection? _conn;
    private DuckDbArrowResultReader? _resultReader;
    private Schema? _resultSchema;
    private IReadOnlyList<PipeColumnInfo>? _columns;

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;
    public Schema? Schema => _resultSchema;
    public IEnumerable<string?> SqlBearingValues => [_query, _initSql];

    public DuckDBSqlProcessor(
        IMemoryChannelRegistry registry,
        string query,
        string mainAlias,
        string mainChannelAlias,
        string[] refAliases,
        string[] refChannelAliases,
        ILogger<DuckDBSqlProcessor> logger,
        string? initSql = null,
        IStringContentResolver? resolver = null,
        bool edgesOnly = false)
    {
        _registry = registry;
        _query = query;
        _statement = query;
        _initSql = initSql;
        _resolver = resolver;
        _mainAlias = mainAlias;
        _mainChannelAlias = mainChannelAlias;
        _refAliases = refAliases;
        _refChannelAliases = refChannelAliases;
        _logger = logger;
        _edgesOnly = edgesOnly;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        _logger.LogDebug("DuckDBSqlProcessor: OpenAsync — query={Query}", _query);
        try
        {
            if (_edgesOnly && !string.IsNullOrWhiteSpace(_initSql))
                throw new InvalidOperationException(
                    "--duck-init is refused under --edges-only: the SQL processor reads the channels its branch declares and nothing else.");

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

                if (_edgesOnly)
                {
                    // No file, database, extension or ATTACH: the query reads the scans registered below, and DuckDB
                    // still spills to its temporary directory. Its threads and memory are sized from what the runtime
                    // reports of the process's own limits (a container's quota, not the host's), which DuckDB's
                    // defaults do not read. The configuration is locked, so the query cannot turn a setting back.
                    cmd.CommandText = "SET enable_external_access = false";
                    await cmd.ExecuteNonQueryAsync(ct);
                    cmd.CommandText = $"SET threads = {Environment.ProcessorCount}";
                    await cmd.ExecuteNonQueryAsync(ct);
                    if (GC.GetGCMemoryInfo().TotalAvailableMemoryBytes is > 0 and var available)
                    {
                        cmd.CommandText = $"SET memory_limit = '{available / 10 * 8 / 1_000_000}MB'";
                        await cmd.ExecuteNonQueryAsync(ct);
                    }
                    cmd.CommandText = "SET lock_configuration = true";
                    await cmd.ExecuteNonQueryAsync(ct);
                }
            }

            await DuckInitSqlRunner.RunAsync(_conn, _initSql, _resolver, ct);

            ValidateAliases();

            if (_refAliases.Length > 0)
                await RegisterRefTablesAsync(ct);

            if (!string.IsNullOrEmpty(_mainAlias))
                await RegisterStreamingTableAsync(_mainAlias, _mainChannelAlias, ct);

            _logger.LogDebug("DuckDBSqlProcessor: Sources registered. Preparing statement...");
            await ApplyProjectionsFromExplainAsync(ct);

            _logger.LogDebug("DuckDBSqlProcessor: Inspecting schema from prepared statement...");
            _resultReader = new DuckDbArrowResultReader(_conn, _statement, _logger);
            _resultSchema = _resultReader.Prepare();

            _columns = ArrowSchemaFactory.ToPipeColumns(_resultSchema).ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DuckDBSqlProcessor: OpenAsync FAILED: {Message}", ex.Message);
            throw;
        }
    }

    // ── Input registration ───────────────────────────────────────────────────────────

    // The --ref sources are drained together (the wait on the sources is the cost), then registered one after
    // the other in the order of _refAliases. A registration writes the maps of the sources (_streamProjections,
    // _viewBodies, _activeStreams, _allocatedPointers) and uses the one connection, so it is one registration
    // at a time on both: two at once lose an entry of the maps, and the query is refused or faults.
    // A source drained and not taken by a registration (a sibling drain failed, an earlier registration
    // failed, the run was cancelled) is no scan's stream, so nothing else releases its batches: they are
    // disposed here, and a source a registration took is left to _activeStreams and Dispose.
    private async Task RegisterRefTablesAsync(CancellationToken ct)
    {
        var drains = _refAliases.Select((_, i) => DrainRefAsync(_refChannelAliases[i], ct)).ToArray();
        try
        {
            await Task.WhenAll(drains);
        }
        catch
        {
            DisposeDrained(drains, from: 0);
            throw;
        }

        for (var i = 0; i < drains.Length; i++)
        {
            var (schema, batches) = drains[i].Result;
            try
            {
                await RegisterArrowStreamAsync(_refAliases[i], new StaticArrowStream(schema, batches), ct);
            }
            catch
            {
                DisposeDrained(drains, from: i + 1);
                throw;
            }
        }
    }

    private static void DisposeDrained(Task<(Schema Schema, List<RecordBatch> Batches)>[] drains, int from)
    {
        for (var i = from; i < drains.Length; i++)
        {
            if (!drains[i].IsCompletedSuccessfully) continue;
            foreach (var batch in drains[i].Result.Batches)
                batch.Dispose();
        }
    }

    // Materialises a --ref source fully into an in-memory list of RecordBatches.
    // Draining is required because ref tables are often joined multiple times.
    private async Task<(Schema Schema, List<RecordBatch> Batches)> DrainRefAsync(string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        ValidateSchema(channelAlias, schema);
        var channelTuple = _registry.GetArrowChannel(channelAlias)
            ?? throw new Exception($"Arrow channel '{channelAlias}' not found");

        var batches = new List<RecordBatch>();
        try
        {
            await foreach (var batch in channelTuple.Channel.Reader.ReadAllAsync(ct))
                batches.Add(batch);
        }
        catch
        {
            foreach (var batch in batches)
                batch.Dispose();
            throw;
        }

        return (schema, batches);
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

    // A scan is registered under "{alias}__raw_cdi", its columns exported behind the stream's prefix, and
    // the user's table is a view that names them back (casting an Arrow extension type to its DuckDB
    // type). A plan then lists a scan's projection by prefixed names, which resolve to exactly one stream.
    private async Task RegisterArrowStreamAsync(string alias, IArrowArrayStream underlyingStream, CancellationToken ct)
    {
        var ordinal = Interlocked.Increment(ref _streamOrdinal);
        var stream = new ProjectedArrowStream(underlyingStream, $"__dtp_s{ordinal}__", alias);
        _streamProjections[alias] = stream;

        var source = stream.SourceSchema;
        foreach (var f in source.FieldsList)
        {
            if (f.HasMetadata && f.Metadata.TryGetValue("ARROW:extension:name", out var ext)
                && GetDuckDBCastForExtension(ext) == null
                && ext != "arrow.opaque" && ext != "arrow.parquet.variant")
            {
                _logger.LogWarning("DuckDBSqlProcessor: Unhandled canonical Arrow extension '{Extension}' on field '{Field}'. Falling back to physical type.", ext, f.Name);
            }
        }

        var scanAlias = $"{alias}__raw_cdi";

        unsafe
        {
            _activeStreams.Add(stream);
            var ffiStreamPtr = (CArrowArrayStream*)Marshal.AllocHGlobal(Marshal.SizeOf<CArrowArrayStream>());
            _allocatedPointers.Add((IntPtr)ffiStreamPtr);
            CArrowArrayStreamExporter.ExportArrayStream(stream, ffiStreamPtr);

            if (DuckDbArrowNative.DuckDBArrowScan(_conn!.NativeConnection, scanAlias, ffiStreamPtr) != DuckDBState.Success)
            {
                // A refused scan left DuckDB no view on the struct, so the stream is released at once.
                _activeStreams.Remove(stream);
                CArrowArrayStreamImporter.ImportArrayStream(ffiStreamPtr).Dispose();
                _allocatedPointers.Remove((IntPtr)ffiStreamPtr);
                Marshal.FreeHGlobal((IntPtr)ffiStreamPtr);
                throw new Exception($"Failed to register Arrow stream scan for '{alias}'");
            }
        }

        var columns = source.FieldsList.Select(f =>
        {
            var exported = QuoteIdentifier(stream.Prefix + f.Name);
            if (f.HasMetadata && f.Metadata.TryGetValue("ARROW:extension:name", out var ext)
                && GetDuckDBCastForExtension(ext) is { } castType)
                return $"CAST({exported} AS {castType}) AS {QuoteIdentifier(f.Name)}";
            return $"{exported} AS {QuoteIdentifier(f.Name)}";
        });
        _viewBodies[alias] = $"SELECT {string.Join(", ", columns)} FROM {QuoteIdentifier(scanAlias)}";
        var viewSql = $"CREATE VIEW {QuoteIdentifier(alias)} AS {_viewBodies[alias]}";

        _logger.LogDebug("DuckDBSqlProcessor: Naming the columns of '{Alias}' back through a view", alias);
        using var cmd = _conn!.CreateCommand();
        cmd.CommandText = viewSql;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static string QuoteIdentifier(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";

    // ── Schema inspection from prepared statement ────────────────────────────────────

    // DuckDB hands an Arrow scan only the columns its plan uses, in the plan's order, with no say for the
    // stream. EXPLAIN lists each ARROW_SCAN's projection on its own; a column is named behind its stream's
    // prefix, so a scan resolves to one stream and its list is that stream's projection. A scan is never
    // merged with another: two streams that carry the same column name would be handed each other's.
    //
    // A stream is read to its end by one scan, so a source scanned twice hands the second scan nothing. A
    // --ref is already held whole: one the plan scans more than once is read once into a MATERIALIZED CTE put
    // in front of the query under its own name. The main is a stream and is refused. A table in place of the
    // CTE would move a join's build side onto the stream, which DuckDB estimates at one row, and a CTE inside
    // the view is copied again at each reference.
    private async Task ApplyProjectionsFromExplainAsync(CancellationToken ct)
    {
        List<List<string>> scans;
        try
        {
            scans = await ExplainScansAsync(_statement, ct);
        }
        catch (Exception ex)
        {
            throw new Exception(
                "DuckDBSqlProcessor: Pre-flight EXPLAIN failed. Aborting query to prevent potential data corruption or native crashes due to DuckDB projection pushdown bugs.", ex);
        }

        var scansByAlias = GroupScansBySource(scans);
        var rescanned = scansByAlias.Where(kv => kv.Value.Count > 1).Select(kv => kv.Key).ToList();
        if (rescanned.Contains(_mainAlias))
            throw ScannedMoreThanOnce(_mainAlias);

        if (rescanned.Count > 0)
        {
            _statement = ReadOnceInFront(_query, rescanned);
            _logger.LogDebug("DuckDBSqlProcessor: the plan scans [{Aliases}] more than once; reading each once through a CTE", string.Join(", ", rescanned));
            try
            {
                scansByAlias = GroupScansBySource(await ExplainScansAsync(_statement, ct));
            }
            catch (Exception ex)
            {
                throw ScannedMoreThanOnce(rescanned[0], ex);
            }
            if (scansByAlias.FirstOrDefault(kv => kv.Value.Count > 1).Key is { } still)
                throw ScannedMoreThanOnce(still);
        }

        foreach (var (alias, scansOfSource) in scansByAlias)
        {
            var columns = scansOfSource[0];
            if (columns.Count == 0) continue;
            _logger.LogDebug("DuckDBSqlProcessor: Setting projection for '{Alias}': [{Cols}]", alias, string.Join(", ", columns));
            _streamProjections[alias].SetProjectedColumns(columns);
        }
    }

    private async Task<List<List<string>>> ExplainScansAsync(string statement, CancellationToken ct)
    {
        var scans = new List<List<string>>();
        using var cmd = _conn!.CreateCommand();
        cmd.CommandText = $"EXPLAIN (FORMAT JSON) {statement}";

        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.FieldCount < 2 || reader.IsDBNull(1)) continue;

            var json = reader.GetValue(1)?.ToString();
            if (string.IsNullOrEmpty(json)) continue;

            ParseProjectionsFromJson(json, scans);
        }
        return scans;
    }

    // Each scan's projection, by the source it reads. A scan that reads no column names no source and is not
    // counted: the stream refuses a second end instead (ProjectedArrowStream).
    private Dictionary<string, List<List<string>>> GroupScansBySource(List<List<string>> scans)
    {
        var bySource = new Dictionary<string, List<List<string>>>();
        foreach (var scan in scans)
        {
            _logger.LogDebug("DuckDBSqlProcessor: ARROW_SCAN projection [{Cols}]", string.Join(", ", scan));

            string? owner = null;
            var columns = new List<string>();
            foreach (var exported in scan)
            {
                var (alias, column) = ResolveExportedColumn(exported);
                if (owner is not null && owner != alias)
                    throw new InvalidOperationException(
                        $"DuckDBSqlProcessor: one scan of the plan reads columns of two sources ('{exported}' and an earlier column). The plan cannot be told apart from another; refusing the query.");
                owner = alias;
                if (!columns.Contains(column)) columns.Add(column);
            }

            if (owner is null) continue;
            if (!bySource.TryGetValue(owner, out var list)) bySource[owner] = list = new();
            list.Add(columns);
        }
        return bySource;
    }

    private static InvalidOperationException ScannedMoreThanOnce(string alias, Exception? inner = null)
        => new($"DuckDBSqlProcessor: the plan scans '{alias}' more than once, and a source is a stream a scan reads to its end: a second scan would find nothing. Read '{alias}' once in a CTE (WITH c AS MATERIALIZED (SELECT * FROM {alias}) ...) and select from that.", inner);

    // What opens a query's own WITH list, past leading whitespace and comments. A query it misses is planned
    // with the CTE prefixed, which fails to bind and is refused: a miss costs a refusal, never a value.
    private static readonly Regex LeadingWith = new(
        @"\A(?:\s|--[^\n]*|/\*.*?\*/)*WITH(?:\s+RECURSIVE)?\b",
        RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant);

    // One MATERIALIZED CTE per alias, under the alias's own name and read off its raw scan (never off the view,
    // which a recursive list would read as the CTE itself), first in the query's own WITH list or in front of
    // it. The query's SELECT is left as written, so its columns and its order are the user's.
    private string ReadOnceInFront(string query, IEnumerable<string> aliases)
    {
        var ctes = string.Join(", ", aliases.Select(a => $"{QuoteIdentifier(a)} AS MATERIALIZED ({_viewBodies[a]})"));
        var opening = LeadingWith.Match(query);
        return opening.Success
            ? $"{query[..opening.Length]} {ctes},{query[opening.Length..]}"
            : $"WITH {ctes}\n{query}";
    }

    // A name in a plan is a column of one stream behind its prefix, or a field of a struct column
    // ("Col._value"), which stands for the whole column: the stream hands over the struct.
    private (string Alias, string Column) ResolveExportedColumn(string exported)
    {
        foreach (var (alias, stream) in _streamProjections)
        {
            if (!exported.StartsWith(stream.Prefix, StringComparison.OrdinalIgnoreCase)) continue;

            var rest = exported[stream.Prefix.Length..];
            var fields = stream.SourceSchema.FieldsList;
            var match = fields.FirstOrDefault(f => string.Equals(f.Name, rest, StringComparison.OrdinalIgnoreCase));
            if (match is null && rest.IndexOf('.') is > 0 and var dot)
            {
                var root = rest[..dot];
                match = fields.FirstOrDefault(f => string.Equals(f.Name, root, StringComparison.OrdinalIgnoreCase));
            }
            if (match is not null) return (alias, match.Name);
        }

        throw new InvalidOperationException(
            $"DuckDBSqlProcessor: the plan reads the column '{exported}', which no registered source carries. Refusing the query rather than reading it from the wrong source.");
    }

    // Appends one projection list per ARROW_SCAN of an EXPLAIN (FORMAT JSON) plan, in the order the plan
    // gives it, which is DuckDB's column_ids order and therefore the position of each column in the batch.
    // A scan in the recursive part of a recursive CTE (the second child of REC_CTE) runs again at each
    // iteration, so it is appended twice: it is a source scanned more than once.
    private void ParseProjectionsFromJson(string json, List<List<string>> scans)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Array)
                foreach (var item in root.EnumerateArray())
                    TraversePlanForProjections(item, scans);
            else if (root.ValueKind == JsonValueKind.Object)
                TraversePlanForProjections(root, scans);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DuckDBSqlProcessor: Failed to parse EXPLAIN JSON");
        }
    }

    private static void TraversePlanForProjections(JsonElement element, List<List<string>> scans, bool repeated = false)
    {
        var name = element.TryGetProperty("name", out var nameProp) ? nameProp.GetString() : null;
        if (name == "ARROW_SCAN" &&
            element.TryGetProperty("extra_info", out var extraInfo) &&
            extraInfo.TryGetProperty("Projections", out var projProp))
        {
            var scan = new List<string>();
            if (projProp.ValueKind == JsonValueKind.Array)
            {
                foreach (var p in projProp.EnumerateArray())
                {
                    var col = p.GetString();
                    if (!string.IsNullOrEmpty(col) && !scan.Contains(col, StringComparer.OrdinalIgnoreCase))
                        scan.Add(col);
                }
            }
            else if (projProp.ValueKind == JsonValueKind.String)
            {
                var col = projProp.GetString();
                if (!string.IsNullOrEmpty(col)) scan.Add(col);
            }
            if (scan.Count > 0)
            {
                scans.Add(scan);
                if (repeated) scans.Add(scan);
            }
        }

        if (element.TryGetProperty("children", out var children) &&
            children.ValueKind == JsonValueKind.Array)
        {
            var index = 0;
            foreach (var child in children.EnumerateArray())
            {
                TraversePlanForProjections(child, scans, repeated || (name == "REC_CTE" && index > 0));
                index++;
            }
        }
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

    // DuckDB goes before the streams it scans. The prepared statement and a streaming result each hold the
    // connection's context, and the context holds the in-memory database: once the read loop has destroyed its
    // result, closing the connection after the reader drops the last of them, which waits for the query's tasks
    // and stops the database's threads. From then on no scan can enter an exported stream. A stream released
    // before that is a freed handle a running scan still calls into, a native fault.
    public void Dispose()
    {
        _resultReader?.Dispose();
        _resultReader = null;

        _conn?.Dispose();
        _conn = null;

        ReleaseStreams();
    }

    public async ValueTask DisposeAsync()
    {
        _resultReader?.Dispose();
        _resultReader = null;

        if (_conn is not null) { await _conn.DisposeAsync(); _conn = null; }

        ReleaseStreams();
    }

    // DuckDB never calls the release of an exported stream: a scan reads a copy of the struct whose release does
    // nothing. Importing the struct and disposing the import calls it, which frees the exporter's handle and
    // disposes the stream; without it the handle keeps the stream and every batch it holds reachable for the
    // life of the process. The struct is freed after its release, which reads it. A stream's dispose runs once
    // whoever calls it, so the pass over _activeStreams finds the released ones done.
    private unsafe void ReleaseStreams()
    {
        foreach (var ptr in _allocatedPointers)
        {
            CArrowArrayStreamImporter.ImportArrayStream((CArrowArrayStream*)ptr).Dispose();
            Marshal.FreeHGlobal(ptr);
        }
        _allocatedPointers.Clear();

        foreach (var stream in _activeStreams)
            stream.Dispose();
        _activeStreams.Clear();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────────

    private static ReadOnlyMemory<object?[]> ConvertBatchToRows(RecordBatch batch)
        => SqlProcessorHelpers.ConvertBatchToRows(batch);

    private void ValidateAliases()
        => SqlProcessorHelpers.ValidateAliases(_mainChannelAlias, _refChannelAliases);

    private static void ValidateSchema(string alias, Schema schema)
        => SqlProcessorHelpers.ValidateSchema(alias, schema);
}
