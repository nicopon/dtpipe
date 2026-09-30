using DtPipe.Lab.Contracts;
using Microsoft.AspNetCore.SignalR;

namespace DtPipe.Lab.Coordinator;

public sealed record NodeView(
    string Name, string Group, string Description, bool Online, IReadOnlyList<DatasetInfo> Datasets,
    IReadOnlyList<FragmentStatus> Fragments, NodeRole Role, IReadOnlyList<BrickInfo> Bricks, bool Sandbox);

/// <summary>
/// The lab hosts it knows of and what each reports about its fragments. <c>INodeRegistry</c> only
/// answers per fragment name; this is the host-level view the page draws.
/// </summary>
public sealed class NodeInventory
{
    private sealed class Entry
    {
        public required NodeAnnouncement Announcement;
        public string? ConnectionId;
        public string? ClientId;
        public Action? Abort;
        public readonly Dictionary<string, FragmentStatus> Fragments = new(StringComparer.Ordinal);
    }

    private readonly object _lock = new();
    private readonly Dictionary<string, Entry> _nodes = new(StringComparer.Ordinal);

    public void Announce(string connectionId, NodeAnnouncement announcement, string clientId, Action abort)
    {
        lock (_lock)
        {
            if (!_nodes.TryGetValue(announcement.Name, out var entry))
                _nodes[announcement.Name] = entry = new Entry { Announcement = announcement };
            entry.Announcement = announcement;
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
                    e.Announcement.Name, e.Announcement.Group, e.Announcement.Description, e.ConnectionId is not null,
                    e.Announcement.Datasets,
                    e.Fragments.Values.OrderBy(f => f.Fragment, StringComparer.Ordinal).ThenBy(f => f.Instance).ToList(),
                    e.Announcement.Role, e.Announcement.Bricks ?? [], e.Announcement.Sandbox))
                .ToList();
        }
    }

    /// <summary>Every dataset variable of every node. The lab runs on one machine, so the coordinator can reach them all.</summary>
    public IReadOnlyDictionary<string, string> AllDatasetVariables()
    {
        lock (_lock)
        {
            var variables = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var dataset in _nodes.Values.SelectMany(e => e.Announcement.Datasets))
                variables[dataset.Variable] = dataset.Path;
            return variables;
        }
    }
}

/// <summary>
/// The lab's control channel (<see cref="LabHubMethods"/>), one connection per node host, each
/// authenticated by the embedded IDP. A host is who its token says: it may announce only the node
/// its identity is bound to, and its group is the identity's, whatever it declares.
/// </summary>
public sealed class LabHub(NodeInventory inventory, RightsStore rights, EventBus bus, ILogger<LabHub> logger) : Hub
{
    public Task Announce(NodeAnnouncement announcement)
    {
        var clientId = EmbeddedIdp.ClientIdOf(Context.User);
        var identity = rights.Find(clientId);
        if (identity is not { Enabled: true } || identity.Node != announcement.Name)
        {
            var reason = identity is null ? $"'{clientId}' is no known identity"
                : !identity.Enabled ? $"'{clientId}' is disabled"
                : $"'{clientId}' may announce {identity.Node ?? "no node"}, not {announcement.Name}";
            logger.LogWarning("Announcement of {Node} refused: {Reason}", announcement.Name, reason);
            bus.Publish("log", new NodeLogLine("coordinator", null, "Error", $"Announcement of {announcement.Name} refused: {reason}."));
            Context.Abort();
            return Task.CompletedTask;
        }
        var context = Context;
        inventory.Announce(Context.ConnectionId, announcement with { Group = identity.Group }, identity.ClientId, context.Abort);
        bus.Publish("nodes", inventory.Snapshot());
        return Task.CompletedTask;
    }

    public Task ReportFragment(FragmentStatus status)
    {
        // A host reports only on the node it announced.
        if (inventory.NodeOf(Context.ConnectionId) != status.Node) return Task.CompletedTask;
        inventory.Update(status);
        bus.Publish("fragment", status);
        bus.Publish("nodes", inventory.Snapshot());
        return Task.CompletedTask;
    }

    public Task ReportLog(NodeLogLine line)
    {
        if (inventory.NodeOf(Context.ConnectionId) != line.Node) return Task.CompletedTask;
        bus.Publish("log", line);
        return Task.CompletedTask;
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        inventory.Disconnected(Context.ConnectionId);
        bus.Publish("nodes", inventory.Snapshot());
        return base.OnDisconnectedAsync(exception);
    }
}
