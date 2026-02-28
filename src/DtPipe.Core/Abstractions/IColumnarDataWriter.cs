using Apache.Arrow;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Extension for writers that can accept data in Arrow columnar format directly.
/// </summary>
public interface IColumnarDataWriter : IDataWriter
{
    /// <summary>
    /// Writes an Arrow RecordBatch to the target.
    /// </summary>
    ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default);

    /// <summary>
    /// If true, the writer takes ownership of the RecordBatch and is responsible for its disposal.
    /// The pipeline orchestrator should NOT dispose of the batch after calling WriteRecordBatchAsync.
    /// </summary>
    bool PrefersOwnershipTransfer => false;
}
