using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace DtPipe.XStreamers.Duck;

/// <summary>
/// Implements IArrowArrayStream by iterating over a pre-loaded list of RecordBatches.
/// This is used for Reference branches in DuckXStreamer which are fully loaded into RAM.
/// </summary>
internal sealed class InMemoryArrowArrayStream : IArrowArrayStream
{
    private readonly IReadOnlyList<RecordBatch> _batches;
    private int _currentIndex;

    public InMemoryArrowArrayStream(Schema schema, IReadOnlyList<RecordBatch> batches)
    {
        Schema = schema;
        _batches = batches;
    }

    public Schema Schema { get; }

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        if (_currentIndex < _batches.Count)
        {
            return new ValueTask<RecordBatch?>(_batches[_currentIndex++]);
        }
        return new ValueTask<RecordBatch?>(result: null);
    }

    public void Dispose() { }
}
