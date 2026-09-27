using DtPipe.Lab.Contracts;
using Microsoft.AspNetCore.SignalR;

namespace DtPipe.Lab.Coordinator;

public sealed record NodeView(
    string Name, string Group, string Description, bool Online, IReadOnlyList<DatasetInfo> Datasets,
    IReadOnlyList<FragmentStatus> Fragments);

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
        public readonly Dictionary<string, FragmentStatus> Fragments = new(StringComparer.Ordinal);
    }

    private readonly object _lock = new();
    private readonly Dictionary<string, Entry> _nodes = new(StringComparer.Ordinal);

    public void Announce(string connectionId, NodeAnnouncement announcement)
    {
        lock (_lock)
        {
            if (!_nodes.TryGetValue(announcement.Name, out var entry))
                _nodes[announcement.Name] = entry = new Entry { Announcement = announcement };
            entry.Announcement = announcement;
            entry.ConnectionId = connectionId;
        }
    }

    public void Disconnected(string connectionId)
    {
        lock (_lock)
        {
            foreach (var entry in _nodes.Values.Where(e => e.ConnectionId == connectionId))
                entry.ConnectionId = null;
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
                    e.Fragments.Values.OrderBy(f => f.Fragment, StringComparer.Ordinal).ThenBy(f => f.Instance).ToList()))
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

/// <summary>The lab's control channel (<see cref="LabHubMethods"/>), one connection per node host.</summary>
public sealed class LabHub(NodeInventory inventory, EventBus bus) : Hub
{
    public Task Announce(NodeAnnouncement announcement)
    {
        inventory.Announce(Context.ConnectionId, announcement);
        bus.Publish("nodes", inventory.Snapshot());
        return Task.CompletedTask;
    }

    public Task ReportFragment(FragmentStatus status)
    {
        inventory.Update(status);
        bus.Publish("fragment", status);
        bus.Publish("nodes", inventory.Snapshot());
        return Task.CompletedTask;
    }

    public Task ReportLog(NodeLogLine line)
    {
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
