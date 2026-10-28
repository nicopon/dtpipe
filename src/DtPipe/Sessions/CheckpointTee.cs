using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Sessions;

/// <summary>
/// Materialises a columnar stream on its way past, without consuming it.
///
/// This is the broadcast situation from <c>DagOrchestrator</c>, with two consumers instead of N:
/// one upstream owner, an extra reference per additional consumer via
/// <see cref="ArrowOwnership.RetainAll"/> — a refcount bump, never a deep copy — and each
/// consumer disposing the reference it was given (.claude/rules/engine-invariants.md › "RecordBatch ownership").
///
/// A checkpoint is a snapshot, not a derivation: nothing here detects staleness. The session TTL
/// is the housekeeping mechanism, and the content-addressed key is what keeps two different
/// pipelines from writing over each other.
/// </summary>
public static class CheckpointTee
{
    /// <summary>
    /// Yields every batch of <paramref name="source"/> unchanged while writing a retained copy
    /// to <paramref name="checkpointKey"/>. The checkpoint is published only if the stream is
    /// consumed to the end — a partial read leaves no checkpoint rather than a truncated one
    /// that would look complete on the next run.
    /// </summary>
    public static async IAsyncEnumerable<RecordBatch> TeeAsync(
        IAsyncEnumerable<RecordBatch> source,
        CheckpointStore store,
        string checkpointKey,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var writer = store.BeginWrite(checkpointKey);

        await foreach (var batch in source.WithCancellation(ct))
        {
            // The writer is shown the batch but does not own it; the retained copy is what it
            // reads from, and it is disposed here as soon as the frame is written.
            var retained = ArrowOwnership.RetainAll(batch);
            try
            {
                await writer.WriteAsync(retained, ct);
            }
            finally
            {
                retained.Dispose();
            }

            yield return batch;
        }

        await writer.CommitAsync(ct);
    }
}
