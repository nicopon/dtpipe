using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Adapters.Shared.Infrastructure.DuckDb;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Expressions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Security;
using DtPipe.Processors.Sql;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;

namespace DtPipe.Processors.DuckDB;

public sealed class DuckDBSqlProcessor : IColumnarStreamReader, ISqlBearing, IDisposable
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
    private readonly bool _edgesOnly;

    private readonly List<DuckDbArrowTableSource> _sources = new();
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
        _initSql = initSql;
        _resolver = resolver;
        _mainAlias = mainAlias;
        _mainChannelAlias = mainChannelAlias;
        _refAliases = refAliases;
        _refChannelAliases = refChannelAliases;
        _logger = logger;
        CArrowArrayReleaseHook.Logger = logger;
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

            DuckDbArrowTableSource? mainSource = null;
            if (!string.IsNullOrEmpty(_mainAlias))
                mainSource = await RegisterStreamingTableAsync(_mainAlias, _mainChannelAlias, ct);

            mainSource?.BeginPrepare();
            try
            {
                _logger.LogDebug("DuckDBSqlProcessor: Inspecting schema from prepared statement...");
                _resultReader = new DuckDbArrowResultReader(_conn, _query, _logger);
                _resultSchema = _resultReader.Prepare();
            }
            finally
            {
                mainSource?.EndPrepare();
            }

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
    // the other in the order of _refAliases on the single DuckDB connection. A drained source whose
    // registration fails before being added to _sources, along with remaining drains, is disposed here.
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
            var taken = false;
            try
            {
                WarnUnhandledExtensions(schema);
                var fnName = NextFunctionName();
                var source = DuckDbArrowTableSource.RegisterRef(_conn!.NativeConnection, fnName, _refAliases[i], schema, batches);
                _sources.Add(source);
                taken = true;
                await CreateSourceViewAsync(_refAliases[i], fnName, ct);
            }
            catch
            {
                if (!taken)
                {
                    foreach (var batch in batches)
                        batch.Dispose();
                }
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
    // Draining is required because ref tables may be scanned multiple times and inform join cardinality.
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

    private async Task<DuckDbArrowTableSource> RegisterStreamingTableAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        ValidateSchema(channelAlias, schema);
        var channelTuple = _registry.GetArrowChannel(channelAlias)
            ?? throw new Exception($"Arrow channel '{channelAlias}' not found");

        WarnUnhandledExtensions(schema);
        var fnName = NextFunctionName();
        var source = DuckDbArrowTableSource.RegisterStream(
            _conn!.NativeConnection, fnName, alias, schema, channelTuple.Channel.Reader, ct);
        _sources.Add(source);
        await CreateSourceViewAsync(alias, fnName, ct);
        return source;
    }

    private string NextFunctionName() => $"__dtp_scan_{Interlocked.Increment(ref _streamOrdinal)}";

    private async Task CreateSourceViewAsync(string alias, string fnName, CancellationToken ct)
    {
        _logger.LogDebug("DuckDBSqlProcessor: Exposing '{Alias}' over table function {Function}", alias, fnName);
        using var cmd = _conn!.CreateCommand();
        cmd.CommandText = $"CREATE VIEW {QuoteIdentifier(alias)} AS SELECT * FROM {fnName}()";
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private void WarnUnhandledExtensions(Schema schema)
    {
        foreach (var f in schema.FieldsList)
        {
            if (f.HasMetadata && f.Metadata.TryGetValue("ARROW:extension:name", out var ext)
                && ext is not ("arrow.uuid" or "arrow.json" or "arrow.bool8" or "arrow.opaque" or "arrow.parquet.variant"))
            {
                _logger.LogWarning(
                    "DuckDBSqlProcessor: Unhandled canonical Arrow extension '{Extension}' on field '{Field}'. Falling back to physical type.",
                    ext, f.Name);
            }
        }
    }

    private static string QuoteIdentifier(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";

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

    // DuckDB goes before the registered table sources. Closing the connection after the reader
    // destroys every scan state and releases exported Arrow slices before sources free their
    // converted schemas, logical types, and --ref batches.
    public void Dispose()
    {
        _resultReader?.Dispose();
        _resultReader = null;

        _conn?.Dispose();
        _conn = null;

        DisposeSources();
    }

    public async ValueTask DisposeAsync()
    {
        _resultReader?.Dispose();
        _resultReader = null;

        if (_conn is not null) { await _conn.DisposeAsync(); _conn = null; }

        DisposeSources();
    }

    private void DisposeSources()
    {
        foreach (var source in _sources)
            source.Dispose();
        _sources.Clear();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────────

    private static ReadOnlyMemory<object?[]> ConvertBatchToRows(RecordBatch batch)
        => SqlProcessorHelpers.ConvertBatchToRows(batch);

    private void ValidateAliases()
        => SqlProcessorHelpers.ValidateAliases(_mainChannelAlias, _refChannelAliases);

    private static void ValidateSchema(string alias, Schema schema)
        => SqlProcessorHelpers.ValidateSchema(alias, schema);
}
