using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator;

/// <summary>
/// One host carrying a node's identity. <paramref name="Node"/> is the node's key, the subject its
/// connections attest (the name it proposes, for a hub that attests none), and <paramref name="Instance"/>
/// the instance id the host chose once for its process, or its connection for a host that sent none: two
/// replicas of one node never share a key, and a reconnection of one is told from a second replica.
/// </summary>
public readonly record struct ReplicaKey(string Node, string Instance)
{
    /// <summary>The start of the instance id, enough for people to tell replicas apart.</summary>
    public string Short => Instance.Length > 8 ? Instance[..8] : Instance;

    public override string ToString() => $"{Node}/{Short}";
}

public sealed class ReplicaRegistryOptions
{
    /// <summary>
    /// How long a replica stays reclaimable after its connection drops before <see cref="ReplicaRegistry.ReplicaLost"/>
    /// fires for it. A SignalR connection reconnects on its own schedule (0/2/10/30s, about 42s) before giving up,
    /// so this must clear that window, or an ordinary reconnect is reported as a lost replica.
    /// </summary>
    public TimeSpan DisconnectGracePeriod { get; init; } = TimeSpan.FromSeconds(45);
}

/// <summary>
/// A replica as the registry holds it at one instant. <see cref="ConnectionId"/> is null while its connection is
/// gone and the grace period has not elapsed: such a replica is known, and reachable by no one.
/// <see cref="Leases"/> are the (run, fragment) pairs the coordinator placed on it.
/// </summary>
public sealed record ReplicaView(
    ReplicaKey Key, string? ConnectionId, NodeAnnouncement Announcement, IReadOnlyList<string> Groups,
    DateTimeOffset FirstAnnouncedAt, IReadOnlyList<(string RunId, string Fragment)> Leases, long LastChosen)
{
    public bool Connected => ConnectionId is not null;
    public Guid? DataPlaneClientId => Announcement.DataPlaneClientId;
}

/// <summary>
/// The one table of the replicas that announced themselves on the node channel: who is present, what each
/// announced, and which runs the coordinator placed on it. A node is online while any of its replicas is
/// connected; a replica whose connection drops waits out its own grace period for the same host to announce
/// again (the same instance id), and is lost only then. A replica that says it is leaving is gone at once.
/// </summary>
public sealed class ReplicaRegistry
{
    private sealed class Replica
    {
        public required ReplicaKey Key;
        public string? ConnectionId;
        public required Action Abort;
        public required NodeAnnouncement Announcement;
        public required IReadOnlyList<string> Groups;
        public DateTimeOffset FirstAnnouncedAt = DateTimeOffset.UtcNow;
        public readonly HashSet<(string RunId, string Fragment)> Leases = [];
        public long LastChosen;
        public CancellationTokenSource? Grace;
    }

    private readonly object _lock = new();
    private readonly Dictionary<ReplicaKey, Replica> _replicas = [];
    private readonly Dictionary<string, ReplicaKey> _byConnection = new(StringComparer.Ordinal);
    private readonly ReplicaRegistryOptions _options;
    private long _sequence;

    public ReplicaRegistry(ReplicaRegistryOptions? options = null)
    {
        _options = options ?? new ReplicaRegistryOptions();
    }

    /// <summary>
    /// Fires once a replica is lost: its grace period elapsed with no announcement from the same host, or it said
    /// it was leaving. The runs placed on it are the ones that end.
    /// </summary>
    public event Action<ReplicaKey>? ReplicaLost;

    /// <summary>
    /// Records a replica on <paramref name="connectionId"/>, or takes it back in place when the same host announces again
    /// (its grace is cancelled, its leases kept). The connection it leaves, if still open, is aborted: it must not outlive
    /// its replacement.
    /// </summary>
    public ReplicaKey Announce(string connectionId, string node, NodeAnnouncement announcement, IReadOnlyList<string> groups, Action abort)
    {
        var key = new ReplicaKey(node, announcement.InstanceId?.ToString("N") ?? "connection-" + connectionId);
        Action? replaced = null;
        lock (_lock)
        {
            if (_replicas.TryGetValue(key, out var existing))
            {
                existing.Grace?.Cancel();
                existing.Grace?.Dispose();
                existing.Grace = null;
                if (existing.ConnectionId is { } previous && previous != connectionId)
                {
                    _byConnection.Remove(previous);
                    replaced = existing.Abort;
                }
                existing.ConnectionId = connectionId;
                existing.Abort = abort;
                existing.Announcement = announcement;
                existing.Groups = groups;
            }
            else
            {
                _replicas[key] = new Replica { Key = key, ConnectionId = connectionId, Abort = abort, Announcement = announcement, Groups = groups };
            }
            _byConnection[connectionId] = key;
        }
        Close(replaced);
        return key;
    }

    /// <summary>Closes a connection. One that is already closed is the usual case, never a failure of the call that found it.</summary>
    private static void Close(Action? abort)
    {
        try { abort?.Invoke(); }
        catch (Exception) { /* closed already */ }
    }

    /// <summary>The replica whose live connection is <paramref name="connectionId"/>; null for a connection that owns none.</summary>
    public ReplicaView? OfConnection(string connectionId)
    {
        lock (_lock)
            return _byConnection.TryGetValue(connectionId, out var key) && _replicas.TryGetValue(key, out var replica) && replica.ConnectionId == connectionId
                ? ViewOf(replica) : null;
    }

    /// <summary>The connection to send <paramref name="replica"/> a message on; null while it is in grace or gone.</summary>
    public string? ConnectionOf(ReplicaKey replica)
    {
        lock (_lock) return _replicas.TryGetValue(replica, out var r) ? r.ConnectionId : null;
    }

    /// <summary>Every replica the registry knows, in grace or not.</summary>
    public IReadOnlyList<ReplicaView> Replicas()
    {
        lock (_lock) return _replicas.Values.Select(ViewOf).ToList();
    }

    /// <summary>Every replica of <paramref name="node"/>, oldest first.</summary>
    public IReadOnlyList<ReplicaView> Replicas(string node)
    {
        lock (_lock) return _replicas.Values.Where(r => r.Key.Node == node).OrderBy(r => r.FirstAnnouncedAt).Select(ViewOf).ToList();
    }

    /// <summary>
    /// The connection a read of <paramref name="node"/> goes to: of its connected replicas, the one with the fewest runs,
    /// then the most recent. Null when none is connected.
    /// </summary>
    public string? ConnectionForRead(string node)
    {
        lock (_lock)
            return _replicas.Values.Where(r => r.Key.Node == node && r.ConnectionId is not null)
                .OrderBy(r => r.Leases.Count).ThenByDescending(r => r.FirstAnnouncedAt).FirstOrDefault()?.ConnectionId;
    }

    /// <summary>The connection of every connected replica of <paramref name="node"/>, oldest first: what a message to the whole node goes to.</summary>
    public IReadOnlyList<(ReplicaKey Key, string ConnectionId)> ConnectionsOf(string node)
    {
        lock (_lock)
            return _replicas.Values.Where(r => r.Key.Node == node && r.ConnectionId is not null).OrderBy(r => r.FirstAnnouncedAt)
                .Select(r => (r.Key, r.ConnectionId!)).ToList();
    }

    /// <summary>True when a replica other than <paramref name="owner"/> announced <paramref name="dataPlaneClientId"/>.</summary>
    public bool DataPlaneClientHeldByOther(Guid dataPlaneClientId, ReplicaKey owner)
    {
        lock (_lock) return _replicas.Values.Any(r => r.Key != owner && r.Announcement.DataPlaneClientId == dataPlaneClientId);
    }

    /// <summary>Drops the control connection of every replica of <paramref name="node"/>: their hosts announce again, with a new token.</summary>
    public int Disconnect(string node)
    {
        List<Action> aborts;
        lock (_lock) aborts = _replicas.Values.Where(r => r.Key.Node == node && r.ConnectionId is not null).Select(r => r.Abort).ToList();
        foreach (var abort in aborts) Close(abort);
        return aborts.Count;
    }

    /// <summary>
    /// The connection of a replica is gone. The replica keeps its place for its grace period, during which the
    /// same host announcing again takes it back; a connection that no replica owns any more (its host announced on a
    /// new one) changes nothing.
    /// </summary>
    public void Disconnected(string connectionId)
    {
        ReplicaKey key;
        CancellationTokenSource? grace = null;
        bool immediately;
        lock (_lock)
        {
            if (!_byConnection.Remove(connectionId, out key)) return;
            if (!_replicas.TryGetValue(key, out var replica) || replica.ConnectionId != connectionId) return;
            replica.ConnectionId = null;
            immediately = _options.DisconnectGracePeriod <= TimeSpan.Zero;
            if (immediately) _replicas.Remove(key);
            else replica.Grace = grace = new CancellationTokenSource();
        }
        if (immediately) ReplicaLost?.Invoke(key);
        else _ = LoseAfterGraceAsync(key, grace!);
    }

    /// <summary>The replica behind <paramref name="connectionId"/> says it is leaving for good: it is gone at once, and its runs end.</summary>
    public void Leave(string connectionId)
    {
        ReplicaKey key;
        lock (_lock)
        {
            if (!_byConnection.Remove(connectionId, out key)) return;
            if (!_replicas.TryGetValue(key, out var replica) || replica.ConnectionId != connectionId) return;
            replica.Grace?.Cancel();
            replica.Grace?.Dispose();
            _replicas.Remove(key);
        }
        ReplicaLost?.Invoke(key);
    }

    /// <summary>
    /// Drops the replicas of <paramref name="node"/> that are not connected, as lost: a node whose identity is gone keeps
    /// none, and the runs placed on them end. A connected replica stays.
    /// </summary>
    public void Forget(string node)
    {
        List<ReplicaKey> dropped = [];
        lock (_lock)
        {
            foreach (var replica in _replicas.Values.Where(r => r.Key.Node == node && r.ConnectionId is null).ToList())
            {
                replica.Grace?.Cancel();
                replica.Grace?.Dispose();
                _replicas.Remove(replica.Key);
                dropped.Add(replica.Key);
            }
        }
        foreach (var key in dropped) ReplicaLost?.Invoke(key);
    }

    private async Task LoseAfterGraceAsync(ReplicaKey key, CancellationTokenSource grace)
    {
        try { await Task.Delay(_options.DisconnectGracePeriod, grace.Token); }
        catch (OperationCanceledException) { return; } // the host announced again, or left: that call disposed this source

        lock (_lock)
        {
            if (!_replicas.TryGetValue(key, out var replica) || !ReferenceEquals(replica.Grace, grace)) return;
            _replicas.Remove(key);
        }
        grace.Dispose();
        ReplicaLost?.Invoke(key);
    }

    /// <summary>
    /// Places every fragment of a run on a replica in one step, or none. For each fragment the connected replicas of
    /// its node that <paramref name="eligible"/> accepts are the candidates, and the one with the fewest runs, then the
    /// least recently chosen, then the lowest instance id, is taken; each choice is leased at once, so the next
    /// fragment sees it. <paramref name="eligible"/> is read in here, with the replica as it is now: what it was
    /// computed from (a connection, a data plane client) is checked by the caller against that replica, so a replica
    /// that changed since is no candidate. Returns null, and leases nothing, when a fragment has no candidate.
    /// </summary>
    public IReadOnlyDictionary<string, ReplicaView>? TryReserve(
        string runId, IReadOnlyList<(string Fragment, string Node)> needs, Func<ReplicaView, string, bool> eligible)
    {
        var chosen = new Dictionary<string, ReplicaView>(StringComparer.Ordinal);
        lock (_lock)
        {
            var taken = new List<(Replica Replica, (string, string) Lease, long Previous)>();
            foreach (var (fragment, node) in needs)
            {
                var pick = _replicas.Values
                    .Where(r => r.Key.Node == node && r.ConnectionId is not null && eligible(ViewOf(r), fragment))
                    .OrderBy(r => r.Leases.Count).ThenBy(r => r.LastChosen).ThenBy(r => r.Key.Instance, StringComparer.Ordinal)
                    .FirstOrDefault();
                if (pick is null)
                {
                    foreach (var (replica, lease, previous) in taken) { replica.Leases.Remove(lease); replica.LastChosen = previous; }
                    return null;
                }
                var key = (runId, fragment);
                taken.Add((pick, key, pick.LastChosen));
                pick.Leases.Add(key);
                pick.LastChosen = ++_sequence;
                chosen[fragment] = ViewOf(pick);
            }
        }
        return chosen;
    }

    /// <summary>Gives back every lease of <paramref name="runId"/>. Idempotent: a replica already lost holds none.</summary>
    public void Release(string runId)
    {
        lock (_lock)
            foreach (var replica in _replicas.Values) replica.Leases.RemoveWhere(l => l.RunId == runId);
    }

    private static ReplicaView ViewOf(Replica r) =>
        new(r.Key, r.ConnectionId, r.Announcement, r.Groups, r.FirstAnnouncedAt, r.Leases.ToList(), r.LastChosen);
}
