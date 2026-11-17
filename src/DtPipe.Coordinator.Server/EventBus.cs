using System.Threading.Channels;

namespace DtPipe.Coordinator.Server;

public sealed record ServerEvent(long Seq, string Type, object Data, DateTimeOffset At);

/// <summary>
/// Fans the server's events out to every open <c>/api/events</c> stream. Keeps a short backlog so a page
/// opened mid-run sees what already happened. A slow subscriber loses its oldest events rather than
/// slowing the publisher. <c>Seq</c> grows by one with each event, in the order every subscriber receives them, and restarts at 1 with each
/// process: <see cref="Boot"/> tells one instance from another, as <c>Seq</c> alone cannot.
/// </summary>
public sealed class EventBus
{
    private const int BacklogSize = 400;

    /// <summary>Names this instance, which is one process: the identity a <see cref="ServerEvent.Seq"/> is read under.</summary>
    public string Boot { get; } = Guid.NewGuid().ToString("N");

    private readonly object _lock = new();
    private readonly LinkedList<ServerEvent> _backlog = new();
    private readonly List<Channel<ServerEvent>> _subscribers = new();
    private readonly Dictionary<string, long> _published = new(StringComparer.Ordinal);
    private long _seq;

    /// <summary>How many events of these types were published since the server started: it moves whenever one of them is, which is all a cache that depends on them needs to know.</summary>
    public long CountOf(IEnumerable<string> types)
    {
        lock (_lock) return types.Sum(t => _published.GetValueOrDefault(t));
    }

    public void Publish(string type, object data)
    {
        lock (_lock)
        {
            _published[type] = _published.GetValueOrDefault(type) + 1;
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
