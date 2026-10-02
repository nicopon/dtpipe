using System.Threading.Channels;

namespace DtPipe.Lab.Coordinator;

public sealed record LabEvent(long Seq, string Type, object Data, DateTimeOffset At);

/// <summary>
/// Fans lab events out to every open <c>/api/events</c> stream. Keeps a short backlog so a page
/// opened mid-run sees what already happened. A slow subscriber loses its oldest events rather than
/// slowing the publisher.
/// </summary>
public sealed class EventBus
{
    private const int BacklogSize = 400;

    private readonly object _lock = new();
    private readonly LinkedList<LabEvent> _backlog = new();
    private readonly List<Channel<LabEvent>> _subscribers = new();
    private long _seq;

    public void Publish(string type, object data)
    {
        lock (_lock)
        {
            var evt = new LabEvent(++_seq, type, data, DateTimeOffset.UtcNow);
            _backlog.AddLast(evt);
            if (_backlog.Count > BacklogSize) _backlog.RemoveFirst();
            foreach (var subscriber in _subscribers) subscriber.Writer.TryWrite(evt);
        }
    }

    public (IReadOnlyList<LabEvent> Backlog, ChannelReader<LabEvent> Live, Action Unsubscribe) Subscribe()
    {
        var channel = Channel.CreateBounded<LabEvent>(new BoundedChannelOptions(1000) { FullMode = BoundedChannelFullMode.DropOldest });
        lock (_lock)
        {
            _subscribers.Add(channel);
            return (_backlog.ToList(), channel.Reader, () => { lock (_lock) _subscribers.Remove(channel); });
        }
    }
}
