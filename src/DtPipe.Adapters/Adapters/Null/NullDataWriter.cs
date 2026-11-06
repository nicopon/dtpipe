using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Adapters.Null;

/// <summary>
/// Discards everything, in whichever mode the pipeline is already in.
///
/// It takes either shape on purpose. This writer exists to measure reader and transformer throughput,
/// and a sink of one shape would make the engine bridge to reach it, which measures the bridge instead of
/// the pipeline.
/// </summary>
public class NullDataWriter : IEitherShapeWriter
{
    public string ComponentName => "null";
    public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> batch, CancellationToken ct = default) => ValueTask.CompletedTask;

    /// <summary>Takes ownership like any columnar writer: Arrow buffers are off-heap, so a
    /// dropped reference is a leak the GC never reports.</summary>
    public ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
    {
        batch.Dispose();
        return ValueTask.CompletedTask;
    }
    public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
        => throw new NotSupportedException("Executing raw commands is not supported for the null writer.");
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
