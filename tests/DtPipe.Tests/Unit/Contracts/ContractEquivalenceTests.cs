using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Contracts;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Services;
using DtPipe.Services.Pipeline;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Contracts;

/// <summary>
/// The acceptance criterion of the executable contract, as a property:
///
///     the schema a DRY RUN captures is the schema a REAL RUN produces.
///
/// <para>
/// It is what makes <c>--dry-run N --contract-save</c> a legitimate CI gesture. If the two ever
/// diverge, a producer publishes a promise its real run does not keep, and every consumer's check
/// is green against the wrong thing — the failure is silent on both sides, which is why this is
/// a test and not a comment.
/// </para>
///
/// <para>
/// The two runs differ in exactly one thing here, the writer, because that is the only thing the
/// engine changes in sample mode. The writer's <em>capability</em> decides row-vs-columnar mode
/// and therefore the segmentation and the bridge count, so substituting a writer of a different
/// shape would test a pipeline neither mode runs.
/// </para>
/// </summary>
public class ContractEquivalenceTests
{
    private const int Rows = 40;
    private const int SampleSize = 10;

    private static readonly PipelineExecutor Executor = new(
        [new DtPipe.Adapters.Infrastructure.Arrow.ArrowRowToColumnarBridgeFactory(
            NullLogger<DtPipe.Core.Infrastructure.Arrow.ArrowRowToColumnarBridge>.Instance)],
        [new DtPipe.Adapters.Infrastructure.Arrow.ArrowColumnarToRowBridgeFactory()],
        NullLogger<PipelineExecutor>.Instance);

    public static TheoryData<string> Pipelines() => new()
    {
        "passthrough",   // the schema the reader published, unchanged
        "adds-column",   // a stage that CHANGES the schema — the case a declared contract misses
    };

    [Theory]
    [MemberData(nameof(Pipelines))]
    public async Task Sample_Run_Captures_What_A_Real_Run_Produces(string pipeline)
    {
        var real = await CaptureAsync(pipeline, sampleMode: false);
        var sample = await CaptureAsync(pipeline, sampleMode: true);

        Assert.NotNull(real);
        Assert.Equal(DataContract.ComputeHash(real!), DataContract.ComputeHash(sample!));
    }

    /// <summary>
    /// The contract describes the pipeline's OUTPUT, not its source. Stated separately because a
    /// tee wired one stage too early would still satisfy the equality above — both runs would be
    /// wrong in the same way.
    /// </summary>
    [Fact]
    public async Task Contract_Describes_The_Last_Stage_Not_The_Reader()
    {
        var captured = await CaptureAsync("adds-column", sampleMode: false);

        Assert.NotNull(captured);
        Assert.Equal(["Id", "Derived"], captured!.FieldsList.Select(f => f.Name).ToArray());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Canonical form — what the hash identifies
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Two schemas carrying the same metadata in different insertion orders are one schema.
    /// </summary>
    /// <remarks>
    /// <see cref="Field.Metadata"/> is a bare dictionary, so its enumeration order follows
    /// insertion. Without a canonical order the hash changes on its own and every consumer's
    /// check turns red for a pipeline nobody touched — a contract that cannot be trusted to mean
    /// "something changed" is worse than no contract.
    /// </remarks>
    [Fact]
    public void Hash_Ignores_Metadata_Insertion_Order()
    {
        var first = SchemaWithMetadata([("b", "2"), ("a", "1"), ("c", "3")]);
        var second = SchemaWithMetadata([("c", "3"), ("a", "1"), ("b", "2")]);

        Assert.Equal(DataContract.ComputeHash(first), DataContract.ComputeHash(second));
    }

    [Fact]
    public void Hash_Still_Separates_Schemas_That_Really_Differ()
    {
        var withUuid = SchemaWithMetadata([("ARROW:extension:name", "arrow.uuid")]);
        var bare = SchemaWithMetadata([]);

        Assert.NotEqual(DataContract.ComputeHash(withUuid), DataContract.ComputeHash(bare));
    }

    [Fact]
    public void Contract_Survives_A_Round_Trip_Through_Its_File()
    {
        var schema = SchemaWithMetadata([("ARROW:extension:name", "arrow.uuid")]);
        var written = DataContract.FromSchema(schema, "batch", "VerbScanOnly", "main");

        var path = Path.Combine(Path.GetTempPath(), $"contract-{Guid.NewGuid():N}.json");
        try
        {
            written.Write(path);
            var read = DataContract.Read(path);

            Assert.Equal(written.Hash, read.Hash);
            Assert.Equal(written.SchemaSource, read.SchemaSource);
            Assert.Equal(written.Enforcement, read.Enforcement);
            Assert.Equal(written.Hash, DataContract.ComputeHash(read.ToArrowSchema()));
        }
        finally
        {
            File.Delete(path);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Harness
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>Runs the engine with a contract tee and returns what it captured.</summary>
    private static async Task<Schema?> CaptureAsync(string pipeline, bool sampleMode)
    {
        var transformers = Build(pipeline);
        await using var reader = new SequenceReader(Rows);
        await reader.OpenAsync(CancellationToken.None);

        var schema = reader.Columns!;
        foreach (var t in transformers) schema = await t.InitializeAsync(schema, CancellationToken.None);

        var segments = DtPipe.Core.Pipelines.PipelineSegmenter.GetSegments(transformers);
        foreach (var s in segments)
        {
            s.InputSchema = reader.Columns!;
            s.OutputSchema = schema;
        }

        var real = new CapturingWriter();
        IDataWriter writer = sampleMode ? SampleModeSink.Wrap(real) : real;

        var tee = new ContractTee();
        var options = sampleMode
            ? new PipelineOptions { BatchSize = 8, Limit = SampleSize, DryRunCount = SampleSize }
            : new PipelineOptions { BatchSize = 8 };

        using var cts = new CancellationTokenSource();
        await Executor.ExecuteSegmentedPipelineAsync(
            reader, writer, segments, schema, options,
            Mock.Of<IExportProgress>(), cts, cts.Token,
            tap: null,
            materialise: src => tee.TeeAsync(src, cts.Token));

        return tee.CapturedSchema;
    }

    private static Schema SchemaWithMetadata((string Key, string Value)[] pairs)
    {
        var metadata = new Dictionary<string, string>();
        foreach (var (key, value) in pairs) metadata[key] = value;
        return new Schema([new Field("Id", Int32Type.Default, nullable: false, metadata)], null);
    }

    private static List<IDataTransformer> Build(string pipeline) => pipeline switch
    {
        "passthrough" => [new PassThroughTransformer()],
        "adds-column" => [new AddsColumnTransformer()],
        _ => throw new ArgumentOutOfRangeException(nameof(pipeline), pipeline, "Unknown pipeline"),
    };

    // ── Doubles ──────────────────────────────────────────────────────────────

    private sealed class SequenceReader : IStreamReader
    {
        private readonly int _count;
        public SequenceReader(int count) => _count = count;
        public IReadOnlyList<PipeColumnInfo>? Columns => new List<PipeColumnInfo> { new("Id", typeof(int), false) };
        public Task OpenAsync(CancellationToken ct = default) => Task.CompletedTask;

        public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
            int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
        {
            for (var i = 0; i < _count; i += batchSize)
            {
                var n = Math.Min(batchSize, _count - i);
                var rows = new object?[n][];
                for (var j = 0; j < n; j++) rows[j] = [i + j];
                yield return rows.AsMemory();
                await Task.Yield();
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class CapturingWriter : IRowDataWriter
    {
        public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class PassThroughTransformer : IDataTransformer
    {
        public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
            => ValueTask.FromResult(columns);
        public object?[]? Transform(IReadOnlyList<object?> row) => row as object?[] ?? row.ToArray();
    }

    /// <summary>Appends a column, so the output schema is not the reader's.</summary>
    private sealed class AddsColumnTransformer : IDataTransformer
    {
        public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
            => ValueTask.FromResult<IReadOnlyList<PipeColumnInfo>>(
                [.. columns, new PipeColumnInfo("Derived", typeof(string), true)]);

        public object?[]? Transform(IReadOnlyList<object?> row) => [.. row, $"d{row[0]}"];
    }
}
