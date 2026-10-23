using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Services.Pipeline;
using DtPipe.Tests.Helpers;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.DryRun;

/// <summary>
/// The sink is what makes "writes cut off" true at the writer boundary. Two properties carry
/// the whole claim: it must never reach the real writer, and it must mirror that writer's
/// capability — because the engine reads row-versus-columnar mode off the writer's interface,
/// a sink of the wrong kind would silently change the segmentation and the run would no longer
/// be the run it is meant to preview.
/// </summary>
public class SampleModeSinkTests
{
    private static readonly IReadOnlyList<PipeColumnInfo> Columns = new List<PipeColumnInfo> { new("Id", typeof(int), false) };

    [Fact]
    public void Wrap_Mirrors_A_Columnar_Writer()
    {
        var sink = SampleModeSink.Wrap(new SpyColumnarWriter());

        sink.Should().BeAssignableTo<IColumnarDataWriter>();
    }

    [Fact]
    public void Wrap_Mirrors_A_Row_Writer_And_Does_Not_Claim_Columnar()
    {
        var sink = SampleModeSink.Wrap(new SpyRowWriter());

        sink.Should().BeAssignableTo<IRowDataWriter>();
        sink.Should().NotBeAssignableTo<IColumnarDataWriter>(
            "claiming columnar over a row writer would add a bridge the real run does not have");
    }

    [Fact]
    public async Task Row_Sink_Never_Reaches_The_Real_Writer()
    {
        var spy = new SpyRowWriter();
        var sink = (IRowDataWriter)SampleModeSink.Wrap(spy);

        await sink.InitializeAsync(Columns);
        await sink.WriteBatchAsync([[1], [2], [3]]);
        await sink.CompleteAsync();
        await sink.ExecuteCommandAsync("TRUNCATE TABLE target");

        spy.Initialized.Should().BeFalse();
        spy.RowsWritten.Should().Be(0);
        spy.Completed.Should().BeFalse();
        spy.Commands.Should().BeEmpty();
        ((ISampleModeSink)sink).RowsWritten.Should().Be(3, "the sink still counts what the pipeline delivered");
    }

    [Fact]
    public async Task Columnar_Sink_Never_Reaches_The_Real_Writer()
    {
        var spy = new SpyColumnarWriter();
        var sink = (IColumnarDataWriter)SampleModeSink.Wrap(spy);

        await sink.InitializeAsync(Columns);
        await sink.WriteRecordBatchAsync(Batch(4));
        await sink.CompleteAsync();

        spy.Initialized.Should().BeFalse();
        spy.BatchesWritten.Should().Be(0);
        spy.Completed.Should().BeFalse();
        ((ISampleModeSink)sink).RowsWritten.Should().Be(4);
    }

    [Fact]
    public async Task Columnar_Sink_Disposes_Every_Batch()
    {
        var pool = new TrackingMemoryPool();
        var sink = (IColumnarDataWriter)SampleModeSink.Wrap(new SpyColumnarWriter());

        for (var i = 0; i < 5; i++)
            await sink.WriteRecordBatchAsync(Batch(8, pool));

        pool.TotalAllocations.Should().BeGreaterThan(0, "the fixture must actually allocate for this to mean anything");
        pool.ActiveAllocations.Should().Be(0,
            "the sink takes ownership like any columnar writer; only dropping the reference would leak off-heap memory the GC never reports");
    }

    [Fact]
    public async Task Disposing_The_Sink_Disposes_The_Uninitialised_Writer()
    {
        var spy = new SpyRowWriter();
        var sink = SampleModeSink.Wrap(spy);

        await sink.DisposeAsync();

        spy.Disposed.Should().BeTrue();
        spy.Initialized.Should().BeFalse("disposing a writer that was never initialised must not create anything");
    }

    // ─────────────────────────────────────────────────────────────────────────

    private static RecordBatch Batch(int rows, TrackingMemoryPool? pool = null)
    {
        var schema = new Schema.Builder().Field(f => f.Name("Id").DataType(Int32Type.Default).Nullable(false)).Build();
        var builder = new Int32Array.Builder();
        for (var i = 0; i < rows; i++) builder.Append(i);
        var array = pool is null ? builder.Build() : builder.Build(pool);
        return new RecordBatch(schema, new IArrowArray[] { array }, rows);
    }

    private sealed class SpyRowWriter : IRowDataWriter
    {
        public bool Initialized;
        public bool Completed;
        public bool Disposed;
        public long RowsWritten;
        public List<string> Commands { get; } = new();

        public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default) { Initialized = true; return ValueTask.CompletedTask; }
        public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default) { RowsWritten += rows.Count; return ValueTask.CompletedTask; }
        public ValueTask CompleteAsync(CancellationToken ct = default) { Completed = true; return ValueTask.CompletedTask; }
        public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) { Commands.Add(command); return ValueTask.CompletedTask; }
        public ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    }

    private sealed class SpyColumnarWriter : IColumnarDataWriter
    {
        public bool Initialized;
        public bool Completed;
        public int BatchesWritten;

        public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default) { Initialized = true; return ValueTask.CompletedTask; }
        public ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default) { BatchesWritten++; batch.Dispose(); return ValueTask.CompletedTask; }
        public ValueTask CompleteAsync(CancellationToken ct = default) { Completed = true; return ValueTask.CompletedTask; }
        public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
