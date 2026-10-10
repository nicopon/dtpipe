using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;

namespace DtPipe.Coordinator.Server;

/// <summary>One host carrying a node's identity: <see cref="Id"/> is the start of its instance id, <see cref="Origin"/> the text it gave.</summary>
public sealed record ReplicaView(string Id, DateTimeOffset Since, string? Origin);

public sealed record NodeView(
    string Name, IReadOnlyList<string> Groups, string Description, bool Online, IReadOnlyList<DatasetInfo> Datasets,
    IReadOnlyList<FragmentStatus> Fragments, NodeRole Role, IReadOnlyList<BrickInfo> Bricks, IReadOnlyList<string> Capabilities,
    string? DtpipeVersion, IReadOnlyList<ReplicaView> Replicas);

/// <summary>
/// The node hosts it knows of and what each reports about its fragments. A node is one identity of the
/// rights and may be carried by several hosts, its replicas, each with its own control connection: the
/// node is online while any replica is connected, and what it announces is read from its oldest connected
/// replica, the one a deployment is sent to. A replica that disconnects leaves the inventory with
/// the fragments it reported; the node keeps its last announcement for as long as its identity exists.
/// </summary>
public sealed class NodeInventory
{
    private sealed class Replica
    {
        /// <summary>The instance id the host chose, or, for a host that sent none, its connection.</summary>
        public required string Key;
        public required string ConnectionId;
        public required Action Abort;
        public required NodeAnnouncement Announcement;
        public DateTimeOffset FirstAnnouncedAt = DateTimeOffset.UtcNow;
        public readonly Dictionary<string, FragmentStatus> Fragments = new(StringComparer.Ordinal);
    }

    private sealed class Entry
    {
        public required NodeAnnouncement Announcement;
        public IReadOnlyList<string> Groups = [];
        public string? ClientId;
        /// <summary>The connected replicas, oldest first.</summary>
        public readonly List<Replica> Replicas = [];
    }

    private readonly object _lock = new();
    private readonly Dictionary<string, Entry> _nodes = new(StringComparer.Ordinal);

    public void Announce(string connectionId, NodeAnnouncement announcement, string clientId, IReadOnlyList<string> groups, Action abort)
    {
        Action? replaced = null;
        lock (_lock)
        {
            if (!_nodes.TryGetValue(announcement.Name, out var entry))
                _nodes[announcement.Name] = entry = new Entry { Announcement = announcement };
            entry.Announcement = announcement;
            entry.Groups = groups;
            entry.ClientId = clientId;

            var key = announcement.InstanceId?.ToString("N") ?? "connection-" + connectionId;
            var replica = entry.Replicas.FirstOrDefault(r => r.Key == key);
            if (replica is null)
            {
                entry.Replicas.Add(new Replica { Key = key, ConnectionId = connectionId, Abort = abort, Announcement = announcement });
            }
            else
            {
                // The same host on a new connection: the old one is dead or about to be, and must not outlive its replacement.
                if (replica.ConnectionId != connectionId) replaced = replica.Abort;
                replica.ConnectionId = connectionId;
                replica.Abort = abort;
                replica.Announcement = announcement;
            }
        }
        Abort(replaced);
    }

    /// <summary>Closes a connection. One that is already closed is the usual case, never a failure of the call that found it.</summary>
    private static void Abort(Action? abort)
    {
        try { abort?.Invoke(); }
        catch (Exception) { /* closed already */ }
    }

    /// <summary>The node a control connection announced itself as, once the coordinator accepted it.</summary>
    public string? NodeOf(string connectionId)
    {
        lock (_lock) return _nodes.Where(n => n.Value.Replicas.Any(r => r.ConnectionId == connectionId)).Select(n => (string?)n.Key).FirstOrDefault();
    }

    /// <summary>Drops the control connection of every replica of every node <paramref name="clientId"/> announced: their hosts reconnect with a new token.</summary>
    public int Disconnect(string clientId)
    {
        List<Action> aborts;
        lock (_lock) aborts = _nodes.Values.Where(e => e.ClientId == clientId).SelectMany(e => e.Replicas).Select(r => r.Abort).ToList();
        foreach (var abort in aborts) Abort(abort);
        return aborts.Count;
    }

    /// <summary>Drops an offline node from the inventory: its identity is gone or named it otherwise. A node with a connected replica stays.</summary>
    public void Forget(string node)
    {
        lock (_lock)
        {
            if (_nodes.TryGetValue(node, out var entry) && entry.Replicas.Count == 0) _nodes.Remove(node);
        }
    }

    /// <summary>The runner a request for the coordinator goes to: one online first, then by name; <c>null</c> when none is known.</summary>
    public NodeView? PreferredRunner() => Snapshot().Where(n => n.Role == NodeRole.Runner)
        .OrderByDescending(n => n.Online).ThenBy(n => n.Name, StringComparer.Ordinal).FirstOrDefault();

    public string? ClientIdOf(string node)
    {
        lock (_lock) return _nodes.TryGetValue(node, out var e) && e.Replicas.Count > 0 ? e.ClientId : null;
    }

    /// <summary>
    /// The host behind <paramref name="connectionId"/> is gone: what it last reported about its fragments
    /// is no longer known to be true (a killed host leaves a fragment "Launched" for good), so it leaves
    /// with it. A host that only lost its connection reports them all again when it announces itself on
    /// reconnecting. A connection that no replica owns any more, because its host announced on a new one,
    /// changes nothing.
    /// </summary>
    public void Disconnected(string connectionId)
    {
        lock (_lock)
        {
            foreach (var entry in _nodes.Values)
            {
                var replica = entry.Replicas.FirstOrDefault(r => r.ConnectionId == connectionId);
                if (replica is null) continue;
                entry.Replicas.Remove(replica);
                if (entry.Replicas.Count == 0) entry.Announcement = replica.Announcement;
            }
        }
    }

    /// <summary>Files what the replica behind <paramref name="connectionId"/> reports; a connection no replica owns reports nothing.</summary>
    public void Update(string connectionId, FragmentStatus status)
    {
        lock (_lock)
        {
            var replica = _nodes.Values.SelectMany(e => e.Replicas).FirstOrDefault(r => r.ConnectionId == connectionId);
            if (replica is null) return;
            var key = $"{status.Fragment}#{status.Instance}";
            if (status.State == FragmentState.Undeployed) replica.Fragments.Remove(key);
            else replica.Fragments[key] = status;
        }
    }

    /// <summary>The connection a deployment, a re-arming or a read goes to: the oldest connected replica's.</summary>
    public string? ConnectionOf(string node)
    {
        lock (_lock) return _nodes.TryGetValue(node, out var e) ? e.Replicas.FirstOrDefault()?.ConnectionId : null;
    }

    /// <summary>The connection of every connected replica of the node, oldest first: what a command to the whole node goes to.</summary>
    public IReadOnlyList<string> ConnectionsOf(string node)
    {
        lock (_lock) return _nodes.TryGetValue(node, out var e) ? e.Replicas.Select(r => r.ConnectionId).ToList() : [];
    }

    /// <summary>What the node hosts currently report for a fragment, every instance and every replica included.</summary>
    public IReadOnlyList<FragmentStatus> Reported(string fragment)
    {
        lock (_lock)
        {
            return _nodes.Values.SelectMany(e => e.Replicas).SelectMany(r => r.Fragments.Values).Where(f => f.Fragment == fragment).ToList();
        }
    }

    public IReadOnlyList<NodeView> Snapshot()
    {
        lock (_lock)
        {
            return _nodes
                .OrderBy(n => n.Key, StringComparer.Ordinal)
                .Select(n =>
                {
                    var (entry, reference) = (n.Value, n.Value.Replicas.FirstOrDefault()?.Announcement ?? n.Value.Announcement);
                    return new NodeView(
                        n.Key, entry.Groups, reference.Description, entry.Replicas.Count > 0, reference.Datasets,
                        entry.Replicas.SelectMany(r => r.Fragments.Values).OrderBy(f => f.Fragment, StringComparer.Ordinal).ThenBy(f => f.Instance).ToList(),
                        reference.Role, reference.Bricks ?? [], reference.Capabilities ?? [], reference.DtpipeVersion,
                        entry.Replicas.Select(r => new ReplicaView(
                            r.Key.Length > 8 ? r.Key[..8] : r.Key, r.FirstAnnouncedAt, r.Announcement.Origin)).ToList());
                })
                .ToList();
        }
    }
}

/// <summary>
/// The node channel (<see cref="NodeChannel"/>), one connection per node host, each authenticated by
/// the trusted IdP. A host is who its token says, and the rights say what that is: the node name and
/// the groups are its identity's, and the name a host proposes for itself is only ever a proposal, shown
/// to an administrator when no identity knows the token's subject. Everything a host reports is filed
/// under the name of the node its connection announced.
/// </summary>
public sealed class NodeChannelHub(
    NodeInventory inventory, RightsStore rights, EnrollmentQueue enrollment, CoordinatorServerOptions options, EventBus bus, ILogger<NodeChannelHub> logger) : Hub
{
    public Task Announce(NodeAnnouncement announcement)
    {
        var clientId = TrustedIdp.SubjectOf(Context.User);
        var identity = rights.Find(clientId);
        if (identity is null)
        {
            Propose(clientId, announcement);
            Context.Abort();
            return Task.CompletedTask;
        }
        if (!identity.Enabled)
        {
            Refuse(identity.Name, $"'{clientId}' is disabled");
            Context.Abort();
            return Task.CompletedTask;
        }
        var context = Context;
        // Where a host runs is its own word, shown to people: cut here and escaped by the page.
        var origin = announcement.Origin is null ? null : EnrollmentQueue.Clean(announcement.Origin, EnrollmentQueue.MaxOrigin);
        inventory.Announce(Context.ConnectionId, announcement with { Name = identity.Name, Origin = origin }, identity.ClientId, identity.Groups, context.Abort);
        bus.Publish("nodes", inventory.Snapshot());
        return Task.CompletedTask;
    }

    /// <summary>A valid token no identity knows: the host waits for an administrator, and nothing else of it is let in.</summary>
    private void Propose(string? subject, NodeAnnouncement announcement)
    {
        var outcome = enrollment.Propose(subject, announcement, EnrollmentQueue.LabelsOf(Context.User, options.IdentityLabelClaims));
        switch (outcome)
        {
            case Proposal.Recorded:
                logger.LogInformation("{Subject} waits for approval as {Node}", subject, announcement.Name);
                bus.Publish("log", new NodeLogLine("coordinator", null, "Warning", $"'{subject}' waits for approval, proposing the node name {EnrollmentQueue.Clean(announcement.Name, EnrollmentQueue.MaxName)}."));
                bus.Publish("rights", new { });
                break;
            case Proposal.Changed:
                bus.Publish("rights", new { });
                break;
            case Proposal.Full:
                logger.LogWarning("{Subject} refused: {Max} hosts already wait for approval", subject, EnrollmentQueue.MaxPending);
                bus.Publish("log", new NodeLogLine("coordinator", null, "Error", $"'{subject}' refused: {EnrollmentQueue.MaxPending} hosts already wait for approval."));
                break;
            case Proposal.Invalid:
                logger.LogWarning("An announcement with no usable subject was refused");
                break;
        }
    }

    private void Refuse(string node, string reason)
    {
        logger.LogWarning("Announcement of {Node} refused: {Reason}", node, reason);
        bus.Publish("log", new NodeLogLine("coordinator", null, "Error", $"Announcement of {node} refused: {reason}."));
    }

    public Task ReportFragment(FragmentStatus status)
    {
        // A host reports only on the node its connection announced, under that node's name.
        if (inventory.NodeOf(Context.ConnectionId) is not { } node) return Task.CompletedTask;
        status = status with { Node = node };
        inventory.Update(Context.ConnectionId, status);
        bus.Publish("fragment", status);
        bus.Publish("nodes", inventory.Snapshot());
        return Task.CompletedTask;
    }

    public Task ReportLog(NodeLogLine line)
    {
        if (inventory.NodeOf(Context.ConnectionId) is not { } node) return Task.CompletedTask;
        bus.Publish("log", line with { Node = node });
        return Task.CompletedTask;
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        var node = inventory.NodeOf(Context.ConnectionId);
        var clientId = node is null ? null : inventory.ClientIdOf(node);
        inventory.Disconnected(Context.ConnectionId);
        // A node whose identity is gone, or now carries another name, is no longer a node of this coordinator.
        if (node is not null && rights.Find(clientId)?.Name != node) inventory.Forget(node);
        bus.Publish("nodes", inventory.Snapshot());
        return base.OnDisconnectedAsync(exception);
    }
}
