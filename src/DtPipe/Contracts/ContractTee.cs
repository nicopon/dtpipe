using System.Runtime.CompilerServices;
using Apache.Arrow;

namespace DtPipe.Contracts;

/// <summary>
/// Reads the schema off a columnar stream on its way past, and changes nothing.
///
/// <para>
/// Unlike <c>CheckpointTee</c> this holds no reference to a <see cref="RecordBatch"/> and retains
/// no buffer: a <see cref="Apache.Arrow.Schema"/> is managed metadata, so keeping it after the
/// batch is disposed is safe, while keeping the batch would make this an owner and put a second
/// dispose on a path that already has exactly one (CLAUDE.md › "RecordBatch ownership").
/// </para>
/// </summary>
public sealed class ContractTee
{
    /// <summary>The schema of the first batch that went past, or null if none did.</summary>
    public Schema? CapturedSchema { get; private set; }

    /// <summary>
    /// Yields every batch of <paramref name="source"/> unchanged, capturing the first one's schema.
    /// </summary>
    /// <remarks>
    /// The schema is taken from the batch rather than from the column list on purpose: a columnar
    /// reader publishes the real thing — <c>StructType</c>, <c>ListType</c>, the extension metadata
    /// that makes a <c>FixedSizeBinary(16)</c> a <c>Guid</c> — where a schema derived from
    /// <c>PipeColumnInfo</c> is flat. A contract built on the flat one would promise a shape the
    /// pipeline does not produce.
    /// </remarks>
    public async IAsyncEnumerable<RecordBatch> TeeAsync(
        IAsyncEnumerable<RecordBatch> source,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var batch in source.WithCancellation(ct))
        {
            CapturedSchema ??= batch.Schema;
            yield return batch;
        }
    }
}
