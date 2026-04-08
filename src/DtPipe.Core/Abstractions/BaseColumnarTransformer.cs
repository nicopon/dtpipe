using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Models;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Base class for columnar transformers that handles safe RecordBatch ownership transfer.
/// This ensures that input batches are properly disposed of when transformed or filtered.
/// </summary>
public abstract class BaseColumnarTransformer : IColumnarTransformer
{
    public virtual bool CanProcessColumnar { get; protected set; }

    /// <summary>
    /// Implementation of IColumnarTransformer.TransformBatchAsync that enforces Consume-and-Own semantics.
    /// </summary>
    public async ValueTask<RecordBatch?> TransformBatchAsync(RecordBatch batch, CancellationToken ct = default)
    {
        // Execute the actual transformation
        var result = await TransformBatchSafeAsync(batch, ct);

        // NOTE: We no longer Dispose(batch) here because many transformers 
        // (Project, Mask, Overwrite) share ArrayData/Buffers with the input batch.
        // Disposing the input batch would invalidate the result batch's data.
        // Ownership management is moved to PipelineExecutor for linear segments
        // and DagOrchestrator for fan-outs.

        return result;
    }

    /// <summary>
    /// Core transformation logic. 
    /// Implementers should return a new RecordBatch, the same incoming batch, or null.
    /// They do NOT need to dispose of the incoming batch here.
    /// </summary>
    protected abstract ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default);

    public virtual async IAsyncEnumerable<RecordBatch> FlushBatchAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        yield break;
    }

    /// <summary>
    /// Row-mode transform. Transformers with <c>CanProcessColumnar = true</c> are always routed
    /// through <see cref="TransformBatchAsync"/> by the pipeline executor and never need to
    /// override this. Only override when <c>CanProcessColumnar</c> can be <c>false</c>
    /// (e.g. Filter with complex expressions, Format with cross-column dependencies).
    /// </summary>
    public virtual object?[]? Transform(IReadOnlyList<object?> row)
        => throw new NotSupportedException(
               $"{GetType().Name} is columnar-only. Ensure CanProcessColumnar=true or override Transform(row).");

    public virtual IEnumerable<object?[]> Flush()
    {
        return Enumerable.Empty<object?[]>();
    }

    public virtual ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
    }

    public virtual ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
