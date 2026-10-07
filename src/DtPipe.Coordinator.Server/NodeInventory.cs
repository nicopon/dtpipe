using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;

namespace DtPipe.Coordinator.Server;

public sealed record NodeView(
    string Name, IReadOnlyList<string> Groups, string Description, bool Online, IReadOnlyList<DatasetInfo> Datasets,
    IReadOnlyList<FragmentStatus> Fragments, NodeRole Role, IReadOnlyList<BrickInfo> Bricks, IReadOnlyList<string> Capabilities);

/// <summary>
/// The node hosts it knows of and what each reports about its fragments. <c>INodeRegistry</c> only
/// answers per fragment name; this is the host-level view the page draws.
/// </summary>
public sealed class NodeInventory
{
    private sealed class Entry
    {
        public required NodeAnnouncement Announcement;
        public IReadOnlyList<string> Groups = [];
        public string? ConnectionId;
        public string? ClientId;
        public Action? Abort;
        public readonly Dictionary<string, FragmentStatus> Fragments = new(StringComparer.Ordinal);
    }

    private readonly object _lock = new();
    private readonly Dictionary<string, Entry> _nodes = new(StringComparer.Ordinal);

    public void Announce(string connectionId, NodeAnnouncement announcement, string clientId, IReadOnlyList<string> groups, Action abort)
    {
        lock (_lock)
        {
            if (!_nodes.TryGetValue(announcement.Name, out var entry))
                _nodes[announcement.Name] = entry = new Entry { Announcement = announcement };
            entry.Announcement = announcement;
            entry.Groups = groups;
            entry.ConnectionId = connectionId;
            entry.ClientId = clientId;
            entry.Abort = abort;
        }
    }

    /// <summary>The node a control connection announced itself as, once the coordinator accepted it.</summary>
    public string? NodeOf(string connectionId)
    {
        lock (_lock) return _nodes.Values.FirstOrDefault(e => e.ConnectionId == connectionId)?.Announcement.Name;
    }

    /// <summary>Drops the control connection of every node <paramref name="clientId"/> announced: its host reconnects with a new token.</summary>
    public int Disconnect(string clientId)
    {
        List<Action> aborts;
        lock (_lock) aborts = _nodes.Values.Where(e => e.ClientId == clientId && e.ConnectionId is not null && e.Abort is not null).Select(e => e.Abort!).ToList();
        foreach (var abort in aborts) abort();
        return aborts.Count;
    }

    /// <summary>Drops an offline node from the inventory: its identity is gone or named it otherwise. A node that is connected stays.</summary>
    public void Forget(string node)
    {
        lock (_lock)
        {
            if (_nodes.TryGetValue(node, out var entry) && entry.ConnectionId is null) _nodes.Remove(node);
        }
    }

    /// <summary>The runner a request for the coordinator goes to: one online first, then by name; <c>null</c> when none is known.</summary>
    public NodeView? PreferredRunner() => Snapshot().Where(n => n.Role == NodeRole.Runner)
        .OrderByDescending(n => n.Online).ThenBy(n => n.Name, StringComparer.Ordinal).FirstOrDefault();

    public string? ClientIdOf(string node)
    {
        lock (_lock) return _nodes.TryGetValue(node, out var e) && e.ConnectionId is not null ? e.ClientId : null;
    }

    /// <summary>
    /// The host behind <paramref name="connectionId"/> is gone: what it last reported about its fragments
    /// is no longer known to be true (a killed host leaves a fragment "Launched" for good), so it is
    /// forgotten. A host that only lost its connection reports every fragment again when it announces
    /// itself on reconnecting.
    /// </summary>
    public void Disconnected(string connectionId)
    {
        lock (_lock)
        {
            foreach (var entry in _nodes.Values.Where(e => e.ConnectionId == connectionId))
            {
                entry.ConnectionId = null;
                entry.Fragments.Clear();
            }
        }
    }

    public void Update(FragmentStatus status)
    {
        lock (_lock)
        {
            if (!_nodes.TryGetValue(status.Node, out var entry)) return;
            var key = $"{status.Fragment}#{status.Instance}";
            if (status.State == FragmentState.Undeployed) entry.Fragments.Remove(key);
            else entry.Fragments[key] = status;
        }
    }

    public string? ConnectionOf(string node)
    {
        lock (_lock) return _nodes.TryGetValue(node, out var e) ? e.ConnectionId : null;
    }

    /// <summary>What the node hosts currently report for a fragment, every instance included.</summary>
    public IReadOnlyList<FragmentStatus> Reported(string fragment)
    {
        lock (_lock)
        {
            return _nodes.Values.SelectMany(e => e.Fragments.Values).Where(f => f.Fragment == fragment).ToList();
        }
    }

    public IReadOnlyList<NodeView> Snapshot()
    {
        lock (_lock)
        {
            return _nodes.Values
                .OrderBy(e => e.Announcement.Name, StringComparer.Ordinal)
                .Select(e => new NodeView(
                    e.Announcement.Name, e.Groups, e.Announcement.Description, e.ConnectionId is not null,
                    e.Announcement.Datasets,
                    e.Fragments.Values.OrderBy(f => f.Fragment, StringComparer.Ordinal).ThenBy(f => f.Instance).ToList(),
                    e.Announcement.Role, e.Announcement.Bricks ?? [], e.Announcement.Capabilities ?? []))
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
        inventory.Announce(Context.ConnectionId, announcement with { Name = identity.Name }, identity.ClientId, identity.Groups, context.Abort);
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
        inventory.Update(status);
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
