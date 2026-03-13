using Apache.Arrow;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Extension for writers that can accept data in Arrow columnar format directly.
/// </summary>
public interface IColumnarDataWriter : IDataWriter
{
    /// <summary>
    /// Writes an Arrow RecordBatch to the target.
    /// The writer takes exclusive ownership of the RecordBatch and is responsible for its disposal.
    /// The pipeline orchestrator will NOT dispose of the batch after calling this method.
    /// </summary>
    ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default);
}
