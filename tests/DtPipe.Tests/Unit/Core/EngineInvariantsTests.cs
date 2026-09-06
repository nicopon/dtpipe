using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Processors.DuckDB;
using DtPipe.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// The three canonical engine cases as CI-gated facts.
/// These drive DagOrchestrator/PipelineExecutor directly (no CLI) with in-memory
/// readers and writers, mirroring the shapes in GoldenDagDefinitions.cs.
/// CLAUDE.md's "run before commit" obligation is enforced here.
/// </summary>
public class EngineInvariantsTests
{
    private const int RowCount = 10;

    // ─────────────────────────────────────────────────────────────────────────
    // In-memory reader / writer stubs
    // ─────────────────────────────────────────────────────────────────────────

    private sealed class GeneratedRowsReader : IStreamReader
    {
        private readonly int _count;
        public GeneratedRowsReader(int count) => _count = count;
        public IReadOnlyList<PipeColumnInfo>? Columns => new List<PipeColumnInfo> { new("Id", typeof(int), false) };
        public Task OpenAsync(CancellationToken ct) => Task.CompletedTask;
        public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
        {
            for (var i = 0; i < _count; i += batchSize)
            {
                var n = Math.Min(batchSize, _count - i);
                var rows = new object?[n][];
                for (var j = 0; j < n; j++) rows[j] = new object?[] { i + j };
                yield return rows.AsMemory();
                await Task.Yield();
            }
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class GeneratedBatchesReader : IColumnarStreamReader
    {
        private readonly int _count;
        private readonly Schema _schema = new Schema.Builder().Field(f => f.Name("Id").DataType(Int32Type.Default)).Build();
        public GeneratedBatchesReader(int count) => _count = count;
        public IReadOnlyList<PipeColumnInfo>? Columns => new List<PipeColumnInfo> { new("Id", typeof(int), false) };
        public Schema? Schema => _schema;
        public Task OpenAsync(CancellationToken ct) => Task.CompletedTask;

        public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([EnumeratorCancellation] CancellationToken ct = default)
        {
            var array = new Int32Array.Builder().AppendRange(Enumerable.Range(0, _count)).Build();
            yield return new RecordBatch(_schema, new IArrowArray[] { array }, _count);
            await Task.Yield();
        }

        public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var batch in ReadRecordBatchesAsync(ct))
            {
                foreach (var memory in DtPipe.Core.Infrastructure.Arrow.ArrowRowConverter.FlattenBatch(batch, batchSize))
                    yield return memory;
            }
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class CountingWriter : IRowDataWriter
    {
        public long Rows { get; private set; }
        public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
        {
            Rows += rows.Count;
            return ValueTask.CompletedTask;
        }
        public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class CountingWriterFactory : IDataWriterFactory
    {
        private readonly ConcurrentDictionary<string, CountingWriter> _writers;
        private readonly string _key;
        public CountingWriterFactory(ConcurrentDictionary<string, CountingWriter> writers, string key)
        {
            _writers = writers;
            _key = key;
        }
        public string ComponentName => "memory";
        public string Category => "Test";
        public Type OptionsType => typeof(InvariantOptions);
        public bool CanHandle(string connectionString) => false;
        public IDataWriter Create(OptionsRegistry registry)
        {
            var writer = new CountingWriter();
            _writers[_key] = writer;
            return writer;
        }
        public IEnumerable<Type> GetSupportedOptionTypes() => System.Array.Empty<Type>();
    }

    private sealed class InvariantOptions : IOptionSet
    {
        public static string Prefix => "memory";
        public static string DisplayName => "In-memory";
    }

    private sealed class GeneratedReaderFactory : IStreamReaderFactory
    {
        private readonly int _count;
        private readonly bool _columnar;
        public GeneratedReaderFactory(int count, bool columnar = false)
        {
            _count = count;
            _columnar = columnar;
        }
        public string ComponentName => "generate";
        public string Category => "Test";
        public Type OptionsType => typeof(InvariantOptions);
        public bool CanHandle(string connectionString) => false;
        public bool RequiresQuery => false;
        public IEnumerable<Type> GetSupportedOptionTypes() => System.Array.Empty<Type>();
        public IStreamReader Create(OptionsRegistry registry)
            => _columnar ? new GeneratedBatchesReader(_count) : new GeneratedRowsReader(_count);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Harness — a minimal branch executor mirroring JobService's wiring
    // ─────────────────────────────────────────────────────────────────────────

    private static readonly PipelineExecutor Executor = new(
        Enumerable.Empty<IRowToColumnarBridgeFactory>(),
        Enumerable.Empty<IColumnarToRowBridgeFactory>(),
        NullLogger<PipelineExecutor>.Instance);

    private static async Task<int> RunLinearBranchAsync(
        IStreamReaderFactory readerFactory,
        IDataWriterFactory writerFactory,
        CancellationToken ct)
    {
        var registry = new OptionsRegistry();
        await using var reader = readerFactory.Create(registry);
        await reader.OpenAsync(ct);
        var columns = reader.Columns ?? throw new InvalidOperationException("Reader produced no columns.");

        await using var writer = writerFactory.Create(registry);
        await writer.InitializeAsync(columns, ct);

        var progress = Mock.Of<IExportProgress>();
        if (writer is IRowDataWriter rowWriter)
        {
            var rows = Executor.ProduceRowStreamAsync(reader, batchSize: 5, limit: 0, samplingRate: 1.0, samplingSeed: null, progress, ct);
            await Executor.ConsumeRowStreamAsync(rows, rowWriter, batchSize: 5, progress, ct);
        }
        else
        {
            throw new InvalidOperationException("Expected a row writer in these tests.");
        }
        await writer.CompleteAsync(ct);
        return 0;
    }

    private static (DagOrchestrator Orchestrator, MemoryChannelRegistry Registry) BuildOrchestrator()
    {
        var registry = new MemoryChannelRegistry();
        var orchestrator = new DagOrchestrator(NullLogger<DagOrchestrator>.Instance, registry, readerFactories: []);
        return (orchestrator, registry);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Canonical case 1: linear pipeline — single branch, no SQL
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Linear_Pipeline_Single_Branch_No_SQL_Completes()
    {
        var counters = new System.Collections.Concurrent.ConcurrentDictionary<string, CountingWriter>();
        var dag = GoldenDagDefinitions.Linear_SingleBranch;
        var (orchestrator, _) = BuildOrchestrator();

        var result = await orchestrator.ExecuteAsync(dag, (_, _, ct) =>
            RunLinearBranchAsync(new GeneratedReaderFactory(RowCount), new CountingWriterFactory(counters, "main"), ct));

        Assert.Equal(0, result);
        Assert.Equal(RowCount, counters["main"].Rows);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Canonical case 2: two-branch DAG — independent sources
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Two_Branch_DAG_Independent_Sources_Completes()
    {
        var counters = new System.Collections.Concurrent.ConcurrentDictionary<string, CountingWriter>();
        var dag = GoldenDagDefinitions.Dag_TwoInputs_OneOutput;
        var (orchestrator, registry) = BuildOrchestrator();

        var result = await orchestrator.ExecuteAsync(dag, (branch, _, ct) =>
        {
            // source1 has no output → intermediate branch writing to its memory channel.
            if (string.IsNullOrEmpty(branch.Output))
                return EmitSourceToChannelAsync(registry, branch.Alias, rows: 5, ct);
            return RunLinearBranchAsync(new GeneratedReaderFactory(5), new CountingWriterFactory(counters, branch.Alias), ct);
        });

        Assert.Equal(0, result);
        Assert.Equal(5, counters["output1"].Rows);
    }

    private static async Task<int> EmitSourceToChannelAsync(IMemoryChannelRegistry reg, string alias, int rows, CancellationToken token)
    {
        var entry = reg.GetArrowChannel(alias)
            ?? throw new InvalidOperationException($"Arrow channel '{alias}' was not pre-registered.");
        var reader = new GeneratedBatchesReader(rows);
        await using (reader)
        {
            await reader.OpenAsync(token);
            await foreach (var batch in reader.ReadRecordBatchesAsync(token))
            {
                reg.UpdateArrowChannelSchema(alias, batch.Schema);
                await entry.Channel.Writer.WriteAsync(batch, token);
            }
        }
        return 0;
    }

    private static async Task<int> ConsumeTransformerAsync(IStreamTransformer transformer, int expectedRows, CancellationToken token)
    {
        long total = 0;
        try
        {
            await transformer.OpenAsync(token);
            await foreach (var batch in transformer.ReadResultsAsync(ct: token))
                total += batch.Length;
        }
        finally
        {
            await transformer.DisposeAsync();
        }
        if (total != expectedRows)
            throw new InvalidOperationException($"SQL processor emitted {total} rows, expected {expectedRows}.");
        return 0;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Canonical case 3: DAG with SQL processor (in-process DuckDB, no CLI)
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DAG_With_SQL_Processor_Completes()
    {
        const int sourceRows = 100;
        var (orchestrator, registry) = BuildOrchestrator();

        var services = new ServiceCollection();
        services.AddSingleton<IMemoryChannelRegistry>(registry);
        services.AddSingleton<Microsoft.Extensions.Logging.ILoggerFactory>(NullLoggerFactory.Instance);
        var serviceProvider = services.BuildServiceProvider();

        var sqlFactory = new DuckDBSqlTransformerFactory();
        var dag = GoldenDagDefinitions.Dag_SourcePlusSqlProcessor;

        var result = await orchestrator.ExecuteAsync(dag, (branch, ctx, ct) =>
        {
            if (!branch.HasStreamTransformer)
            {
                return EmitSourceToChannelAsync(registry, branch.Alias, sourceRows, ct);
            }

            // Stream-transformer branch: resolve through the real factory + processor.
            var transformer = sqlFactory.Create(
                new[] { "--from", "src", "--sql", "SELECT Id FROM src" },
                ctx,
                serviceProvider);
            return ConsumeTransformerAsync(transformer, sourceRows, ct);
        });

        Assert.Equal(0, result);
    }
}
