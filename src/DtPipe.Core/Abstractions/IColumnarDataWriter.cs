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
}
