using System.Threading.Channels;

namespace DtPipe.Core.Pipelines;

/// <summary>
/// Helper to batch rows into arrays before writing to a channel.
/// </summary>
internal sealed class BatchChannelWriter : IAsyncDisposable
{
    private readonly ChannelWriter<object?[][]> _target;
    private readonly int _batchSize;
    private readonly List<object?[]> _buffer;
    private readonly CancellationToken _ct;

    public BatchChannelWriter(ChannelWriter<object?[][]> target, int batchSize, CancellationToken ct)
    {
        _target = target;
        _batchSize = batchSize;
        _buffer = new List<object?[]>(batchSize);
        _ct = ct;
    }

    public async ValueTask WriteAsync(object?[] row)
    {
        _buffer.Add(row);
        if (_buffer.Count >= _batchSize)
        {
            await FlushAsync();
        }
    }

    public async ValueTask FlushAsync()
    {
        if (_buffer.Count == 0) return;
        // Write a copy of the buffer as an array to the channel
        await _target.WriteAsync(_buffer.ToArray(), _ct);
        _buffer.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        await FlushAsync();
    }
}
