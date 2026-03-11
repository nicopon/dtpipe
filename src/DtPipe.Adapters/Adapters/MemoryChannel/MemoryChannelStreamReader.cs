using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace DtPipe.Adapters.MemoryChannel;

/// <summary>
/// A specialized stream reader that pulls rows from an in-memory channel.
/// Used for orchestrating DAG branches where a downstream component (like an XStreamer)
/// consumes data from an upstream branch.
/// </summary>
public class MemoryChannelStreamReader : IStreamReader
{
    private readonly ChannelReader<IReadOnlyList<object?[]>> _reader;
    private readonly IReadOnlyList<PipeColumnInfo> _columns;

    public MemoryChannelStreamReader(ChannelReader<IReadOnlyList<object?[]>> reader, IReadOnlyList<PipeColumnInfo> columns)
    {
        _reader = reader;
        _columns = columns;
    }

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;

    public Task OpenAsync(CancellationToken ct = default)
    {
        // No IO initialization required
        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        // Iterate through all batches pushed into the channel until it's completed
        await foreach (var batch in _reader.ReadAllAsync(ct))
        {
            // The pipeline expects ReadOnlyMemory<object?[]>
            yield return new ReadOnlyMemory<object?[]>(batch.ToArray());
        }
    }

    public ValueTask DisposeAsync()
    {
        // Consumer doesn't close the channel, that's the producer's job.
        return ValueTask.CompletedTask;
    }
}
