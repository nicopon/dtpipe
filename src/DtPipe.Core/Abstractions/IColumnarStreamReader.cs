using Apache.Arrow;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Extension for readers that can yield data in Arrow columnar format.
/// </summary>
public interface IColumnarStreamReader : IStreamReader
{
    /// <summary>
    /// Reads data as a stream of Arrow RecordBatches.
    /// </summary>
    IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets the Arrow schema of the stream.
    /// </summary>
    Schema? Schema { get; }
}
