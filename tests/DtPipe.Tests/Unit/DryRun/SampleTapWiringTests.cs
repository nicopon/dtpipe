using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.DryRun;
using DtPipe.Services;
using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.DryRun;

/// <summary>
/// The tap's stage numbering is the contract between the engine and everything that reads a
/// capture: 0 is the reader, 1..n follow the transformers in pipeline order. Getting it wrong
/// mislabels every stage of every report, silently — so it is pinned here rather than
/// discovered later through a confusing trace.
/// </summary>
public class SampleTapWiringTests
{
    private static readonly PipelineExecutor Executor = new(
        Enumerable.Empty<IRowToColumnarBridgeFactory>(),
        Enumerable.Empty<IColumnarToRowBridgeFactory>(),
        NullLogger<PipelineExecutor>.Instance);

    private static readonly IReadOnlyList<PipeColumnInfo> IdSchema = new List<PipeColumnInfo> { new("Id", typeof(int), false) };

    [Fact]
    public async Task Row_Chain_Numbers_Reader_Zero_Then_Transformers_In_Order()
    {
        var pipeline = new List<IDataTransformer> { new Add(10), new Add(100) };
        var run = await RunAsync(new RowReader(4), new RowSink(), pipeline, quota: 4);

        run.Stages.Select(s => s.Index).Should().Equal(0, 1, 2);
        Rows(run, 0).Should().Equal(0, 1, 2, 3);
        // stage 1 is after the first transformer, stage 2 after the second
        Rows(run, 1).Should().Equal(10, 11, 12, 13);
        Rows(run, 2).Should().Equal(110, 111, 112, 113);
    }

    [Fact]
    public async Task Columnar_Chain_Numbers_Stages_The_Same_Way()
    {
        var pipeline = new List<IDataTransformer> { new ColumnarAdd(10), new ColumnarAdd(100) };
        var run = await RunAsync(new ColumnarReader(4), new ColumnarSink(), pipeline, quota: 4);

        run.Stages.Select(s => s.Index).Should().Equal(0, 1, 2);
        Rows(run, 0).Should().Equal(0, 1, 2, 3);
        Rows(run, 1).Should().Equal(10, 11, 12, 13);
        Rows(run, 2).Should().Equal(110, 111, 112, 113);
    }

    [Fact]
    public async Task Reader_Only_Pipeline_Still_Captures_Stage_Zero()
    {
        var run = await RunAsync(new RowReader(3), new RowSink(), new List<IDataTransformer>(), quota: 3);

        run.Stages.Should().ContainSingle().Which.Index.Should().Be(0);
        run.Stages[0].TotalSeen.Should().Be(3, "a pipeline with no transformers still has a source to show");
    }

    // ─────────────────────────────────────────────────────────────────────────

    private static List<int> Rows(SampleRun run, int stage)
        => run.Stages.Single(s => s.Index == stage).Rows.Select(r => Convert.ToInt32(r[0])).ToList();

    private static async Task<SampleRun> RunAsync(
        IStreamReader reader, IDataWriter writer, List<IDataTransformer> pipeline, int quota)
    {
        await reader.OpenAsync(CancellationToken.None);

        var schema = reader.Columns!;
        var tap = new SampleTapRecorder(quota);
        tap.OnStageSchema(0, "reader", schema, reader is IColumnarStreamReader);

        for (var i = 0; i < pipeline.Count; i++)
        {
            schema = await pipeline[i].InitializeAsync(schema, CancellationToken.None);
            tap.OnStageSchema(i + 1, pipeline[i].GetType().Name, schema, isColumnar: false);
        }

        var segments = DtPipe.Core.Pipelines.PipelineSegmenter.GetSegments(pipeline);
        foreach (var s in segments) { s.InputSchema = reader.Columns!; s.OutputSchema = schema; }

        using var cts = new CancellationTokenSource();
        await Executor.ExecuteSegmentedPipelineAsync(
            reader, writer, segments, schema, new PipelineOptions { BatchSize = 2 },
            Mock.Of<IExportProgress>(), cts, cts.Token, tap);

        return tap.Build(0, 0);
    }

    private sealed class Add : IDataTransformer
    {
        private readonly int _by;
        public Add(int by) => _by = by;
        public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> c, CancellationToken ct = default) => ValueTask.FromResult(c);
        public object?[]? Transform(IReadOnlyList<object?> row) => [Convert.ToInt32(row[0]) + _by];
    }

    private sealed class ColumnarAdd : BaseColumnarTransformer
    {
        private readonly int _by;
        public ColumnarAdd(int by) { _by = by; CanProcessColumnar = true; }
        public override bool CanProcessColumnar => true;
        protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
        {
            var src = (Int32Array)batch.Column(0);
            var b = new Int32Array.Builder();
            for (var i = 0; i < batch.Length; i++) b.Append(src.GetValue(i)!.Value + _by);
            return ValueTask.FromResult<RecordBatch?>(new RecordBatch(batch.Schema, new IArrowArray[] { b.Build() }, batch.Length));
        }
    }

    private sealed class RowReader : IStreamReader
    {
        private readonly int _n;
        public RowReader(int n) => _n = n;
        public IReadOnlyList<PipeColumnInfo>? Columns => IdSchema;
        public Task OpenAsync(CancellationToken ct = default) => Task.CompletedTask;
        public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
        {
            var rows = Enumerable.Range(0, _n).Select(i => new object?[] { i }).ToArray();
            yield return rows.AsMemory();
            await Task.CompletedTask;
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ColumnarReader : IColumnarStreamReader
    {
        private readonly int _n;
        private readonly Schema _schema = new Schema.Builder().Field(f => f.Name("Id").DataType(Int32Type.Default).Nullable(false)).Build();
        public ColumnarReader(int n) => _n = n;
        public IReadOnlyList<PipeColumnInfo>? Columns => IdSchema;
        public Schema? Schema => _schema;
        public Task OpenAsync(CancellationToken ct = default) => Task.CompletedTask;
        public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([EnumeratorCancellation] CancellationToken ct = default)
        {
            var b = new Int32Array.Builder().AppendRange(Enumerable.Range(0, _n));
            yield return new RecordBatch(_schema, new IArrowArray[] { b.Build() }, _n);
            await Task.CompletedTask;
        }
        public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var batch in ReadRecordBatchesAsync(ct))
                foreach (var m in DtPipe.Core.Infrastructure.Arrow.ArrowRowConverter.FlattenBatch(batch, batchSize))
                    yield return m;
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class RowSink : IRowDataWriter
    {
        public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> c, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ColumnarSink : IColumnarDataWriter
    {
        public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> c, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default) { batch.Dispose(); return ValueTask.CompletedTask; }
        public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
