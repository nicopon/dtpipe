using System.Threading.Channels;

namespace DtPipe.Coordinator.Server;

public sealed record ServerEvent(long Seq, string Type, object Data, DateTimeOffset At);

/// <summary>
/// Fans the server's events out to every open <c>/api/events</c> stream. Keeps a short backlog so a page
/// opened mid-run sees what already happened. A slow subscriber loses its oldest events rather than
/// slowing the publisher.
/// </summary>
public sealed class EventBus
{
    private const int BacklogSize = 400;

    private readonly object _lock = new();
    private readonly LinkedList<ServerEvent> _backlog = new();
    private readonly List<Channel<ServerEvent>> _subscribers = new();
    private long _seq;

    public void Publish(string type, object data)
    {
        lock (_lock)
        {
            var evt = new ServerEvent(++_seq, type, data, DateTimeOffset.UtcNow);
            _backlog.AddLast(evt);
            if (_backlog.Count > BacklogSize) _backlog.RemoveFirst();
            foreach (var subscriber in _subscribers) subscriber.Writer.TryWrite(evt);
        }
    }

    public (IReadOnlyList<ServerEvent> Backlog, ChannelReader<ServerEvent> Live, Action Unsubscribe) Subscribe()
    {
        var channel = Channel.CreateBounded<ServerEvent>(new BoundedChannelOptions(1000) { FullMode = BoundedChannelFullMode.DropOldest });
        lock (_lock)
        {
            _subscribers.Add(channel);
            return (_backlog.ToList(), channel.Reader, () => { lock (_lock) _subscribers.Remove(channel); });
        }
    }
}
