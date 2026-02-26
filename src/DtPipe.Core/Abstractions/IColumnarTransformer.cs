using Apache.Arrow;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Interface for transformers that can process Apache Arrow RecordBatches directly.
/// Implementing this interface enables the Zero-Copy fast-path in the pipeline,
/// avoiding expensive per-row allocations and conversions when possible.
/// </summary>
public interface IColumnarTransformer : IDataTransformer
{
    /// <summary>
    /// Transforms a RecordBatch. Returns the transformed batch, or null if the entire batch is dropped.
    /// Note: Implementation should be as zero-copy as possible by reusing existing arrays if they are not modified.
    /// </summary>
    ValueTask<RecordBatch?> TransformBatchAsync(RecordBatch batch, CancellationToken ct = default);
}
