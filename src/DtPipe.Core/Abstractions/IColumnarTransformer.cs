using System.Runtime.CompilerServices;
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
    /// Indicates whether this transformer can currently process data in columnar mode,
    /// based on its configuration after InitializeAsync has been called.
    /// If false, the orchestrator must use the Row-based Transform() path.
    /// </summary>
    bool CanProcessColumnar { get; }

    /// <summary>
    /// Transforms a RecordBatch. Returns the transformed batch, or null if the entire batch is dropped.
    /// The transformer takes exclusive ownership of the input RecordBatch.
    /// It MUST Dispose() the input batch if it returns a different batch or null.
    /// If it returns the input batch unchanged, ownership is transferred back to the caller.
    /// </summary>
    ValueTask<RecordBatch?> TransformBatchAsync(RecordBatch batch, CancellationToken ct = default);

    /// <summary>
    /// Flushes any buffered batches at the end of the stream.
    /// Stateful transformers should implement this.
    /// </summary>
#pragma warning disable CS1998
    async IAsyncEnumerable<RecordBatch> FlushBatchAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        yield break;
    }
#pragma warning restore CS1998
}
