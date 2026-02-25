using Apache.Arrow;
using Apache.Arrow.Ipc;
using System.Threading.Channels;

namespace DtPipe.XStreamers.Duck;

/// <summary>
/// Implements IArrowArrayStream by pulling RecordBatches from a ChannelReader.
/// This allows DuckDB to consume a DtPipe memory channel in "pull" mode via
/// the Arrow C Stream Interface, enabling true streaming without loading all data into RAM.
/// </summary>
internal sealed class MemoryArrowArrayStream : IArrowArrayStream
{
    private readonly ChannelReader<RecordBatch> _reader;
    private bool _done;

    public MemoryArrowArrayStream(Schema schema, ChannelReader<RecordBatch> reader)
    {
        Schema = schema;
        _reader = reader;
    }

    public Schema Schema { get; }

    public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        if (_done) return null;

        if (await _reader.WaitToReadAsync(cancellationToken))
        {
            if (_reader.TryRead(out var batch)) return batch;
        }

        _done = true;
        return null;
    }

    public void Dispose() { /* canal géré par DagOrchestrator */ }
}
