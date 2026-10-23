using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Base class for columnar transformers. See <c>CLAUDE.md</c> › "RecordBatch ownership
/// (columnar path)" for the disposal contract the segment runner and these implementations share.
/// </summary>
public abstract class BaseColumnarTransformer : IColumnarTransformer
{
    public virtual bool CanProcessColumnar { get; protected set; }
    protected Schema? InputSchema { get; private set; }

    public ValueTask<RecordBatch?> TransformBatchAsync(RecordBatch batch, CancellationToken ct = default)
        => TransformBatchSafeAsync(batch, ct);

    /// <summary>
    /// Core transformation logic. Return one of:
    /// <list type="bullet">
    /// <item>the same <paramref name="batch"/> reference — pure pass-through; the caller keeps
    /// owning it and disposes it once;</item>
    /// <item>a new <see cref="RecordBatch"/> — the caller disposes <paramref name="batch"/> after
    /// this returns, so any input column reused in the result MUST be wrapped in
    /// <see cref="ArrowOwnership.RetainArray"/>;</item>
    /// <item><c>null</c> — the batch is dropped; the caller disposes <paramref name="batch"/>.</item>
    /// </list>
    /// </summary>
    protected abstract ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default);

    public virtual async IAsyncEnumerable<RecordBatch> FlushBatchAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.CompletedTask;
        yield break;
    }

    /// <summary>
    /// Row-mode transform. Transformers with <c>CanProcessColumnar = true</c> are always routed
    /// through <see cref="TransformBatchAsync"/> by the pipeline executor and never need to
    /// override this. Only override when <c>CanProcessColumnar</c> can be <c>false</c>
    /// (e.g. Filter with complex expressions, Format with cross-column dependencies).
    /// </summary>
    public virtual object?[]? Transform(IReadOnlyList<object?> row)
    {
        if (!CanProcessColumnar)
            throw new NotSupportedException($"{GetType().Name} does not support row mode.");

        if (InputSchema == null)
            throw new InvalidOperationException($"{GetType().Name} must be initialized before calling Transform(row).");

        // Use standard converter to create a 1-row batch
        using var batch = ArrowRowConverter.ToRecordBatch(InputSchema, new[] { row }, 1);

        // Execute transformation (synchronous wait is acceptable for row-mode fallback/dry-run)
        using var resultBatch = TransformBatchAsync(batch).GetAwaiter().GetResult();

        if (resultBatch == null || resultBatch.Length == 0)
            return null;

        // Use standard converter to extract the row
        return ArrowRowConverter.ToRow(resultBatch, 0);
    }

    public virtual IEnumerable<object?[]> Flush()
    {
        return Enumerable.Empty<object?[]>();
    }

    public virtual ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        InputSchema = ArrowSchemaFactory.Create(columns);
        return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
    }

    public virtual ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
