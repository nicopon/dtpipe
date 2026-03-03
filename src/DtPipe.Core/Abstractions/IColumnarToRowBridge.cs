using Apache.Arrow;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Bridge that converts Apache Arrow RecordBatch data back into row-based object arrays.
/// This is used when a columnar segment of the pipeline is followed by a row-based segment.
/// </summary>
public interface IColumnarToRowBridge
{
    /// <summary>
    /// Converts a RecordBatch into an asynchronous stream of rows.
    /// </summary>
    IAsyncEnumerable<object?[]> ConvertBatchToRowsAsync(RecordBatch batch, CancellationToken ct = default);
}
