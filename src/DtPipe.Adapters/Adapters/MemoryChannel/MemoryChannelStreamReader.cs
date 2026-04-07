using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Apache.Arrow;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Adapters.MemoryChannel;

/// <summary>
/// A specialized stream reader that pulls rows from an in-memory channel.
/// Used for orchestrating DAG branches where a downstream component (like a processor)
/// consumes data from an upstream branch.
/// OpenAsync waits for the producing branch to publish its schema, which avoids a race
/// condition where fan-out sub-channels are registered with an empty schema snapshot.
/// </summary>
public class MemoryChannelStreamReader : IStreamReader
{
    private readonly ChannelReader<RecordBatch> _reader;
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _alias;
    private IReadOnlyList<PipeColumnInfo>? _columns;

    public MemoryChannelStreamReader(ChannelReader<RecordBatch> reader, IMemoryChannelRegistry registry, string alias)
    {
        _reader = reader;
        _registry = registry;
        _alias = alias;
    }

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;

    public async Task OpenAsync(CancellationToken ct = default)
    {
        // Wait for the producing branch to publish its output schema.
        // This is necessary for fan-out sub-channels (e.g. src__fan_0) which are
        // pre-registered with an empty schema and only populated after the source
        // branch calls UpdateChannelColumns().
        _columns = await _registry.WaitForChannelColumnsAsync(_alias, ct);
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var bridge = new ArrowColumnarToRowBridge();
        var buffer = new List<object?[]>(batchSize);

        // Iterate through all Arrow batches pushed into the channel until it's completed
        await foreach (var batch in _reader.ReadAllAsync(ct))
        {
            // Convert each Arrow batch back to row-based format using the bridge
            await foreach (var row in bridge.ConvertBatchToRowsAsync(batch, ct))
            {
                buffer.Add(row);
                if (buffer.Count >= batchSize)
                {
                    yield return new ReadOnlyMemory<object?[]>(buffer.ToArray());
                    buffer.Clear();
                }
            }
        }

        if (buffer.Count > 0)
        {
            yield return new ReadOnlyMemory<object?[]>(buffer.ToArray());
        }
    }

    public ValueTask DisposeAsync()
    {
        // Consumer doesn't close the channel, that's the producer's job.
        return ValueTask.CompletedTask;
    }
}
