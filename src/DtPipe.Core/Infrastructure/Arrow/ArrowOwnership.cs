using Apache.Arrow;
using Apache.Arrow.Memory;

namespace DtPipe.Core.Infrastructure.Arrow;

/// <summary>
/// Shared-ownership helpers for the columnar pipeline. See <c>CLAUDE.md</c> ›
/// "RecordBatch ownership (columnar path)" for the contract these enforce.
///
/// A <see cref="RecordBatch"/> has one owner at a time; the owner calls <c>Dispose()</c>
/// exactly once. When a stage emits a new batch that reuses another batch's column buffers, it must
/// retain those columns so the two batches can be disposed independently — <see cref="ArrayData.Retain"/>
/// bumps a reference count on the underlying buffers rather than copying them.
/// </summary>
public static class ArrowOwnership
{
    /// <summary>
    /// Returns a view over <paramref name="array"/> that keeps its buffers alive via reference
    /// counting. Dispose the returned array (or the batch that holds it) when done.
    /// </summary>
    public static IArrowArray RetainArray(IArrowArray array)
        => global::Apache.Arrow.ArrowArrayFactory.BuildArray(array.Data.Retain());

    /// <summary>
    /// Returns a new <see cref="RecordBatch"/> whose columns are retained views over
    /// <paramref name="batch"/>'s columns. The source and the returned batch can be disposed
    /// independently. Used for fan-out, where one upstream batch feeds several consumers.
    /// </summary>
    public static RecordBatch RetainAll(RecordBatch batch)
    {
        int columnCount = batch.Schema.FieldsList.Count;
        var arrays = new IArrowArray[columnCount];
        for (int i = 0; i < columnCount; i++)
            arrays[i] = RetainArray(batch.Column(i));
        return new RecordBatch(batch.Schema, arrays, batch.Length);
    }

    /// <summary>
    /// Re-homes a batch read from Arrow IPC onto buffers that carry their own reference count, and
    /// releases the message body it was read into. Call it on every batch an IPC reader hands to
    /// the pipeline.
    /// </summary>
    /// <remarks>
    /// <b>An IPC batch does not obey the contract the rest of the pipeline is written against.</b>
    /// Its column buffers hold no shared handle; the whole message body is a single allocation
    /// owned by the <see cref="RecordBatch"/>. So <see cref="RetainArray"/> bumps nothing, and the
    /// segment runner's dispose of the input frees the body under an output that aliases it.
    /// Reading it back then dereferences freed native memory: <c>--mask</c>, <c>--fake</c>,
    /// <c>--null</c> and <c>--format</c> each crashed with a <c>NullReferenceException</c> raised
    /// from inside Arrow, on an <c>arrow:</c> source and on <c>--from-checkpoint</c>, while the same
    /// transformers over <c>parquet:</c> were correct.
    ///
    /// <para>
    /// The machinery for sharing a buffer is <c>internal</c> to Apache.Arrow, so the handle cannot
    /// be attached from here: the copy is what buys the contract. It is one allocator copy per
    /// batch — measured at roughly three times the cost of reading the batch, ~3 ns per row — paid
    /// on the IPC readers only, and it is what lets every consumer downstream obey one rule instead
    /// of asking where its batch came from.
    /// </para>
    /// </remarks>
    public static RecordBatch TakeOwnership(RecordBatch batch)
    {
        using (batch)
        {
            var allocator = MemoryAllocator.Default.Value;
            int columnCount = batch.Schema.FieldsList.Count;
            var columns = new IArrowArray[columnCount];
            for (int i = 0; i < columnCount; i++)
                columns[i] = global::Apache.Arrow.ArrowArrayFactory.BuildArray(batch.Column(i).Data.Clone(allocator));

            return new RecordBatch(batch.Schema, columns, batch.Length);
        }
    }
}
