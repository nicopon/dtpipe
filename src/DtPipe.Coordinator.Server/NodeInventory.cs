using System.Security.Cryptography;
using System.Text;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// One host carrying a node's identity, as a page shows it. <see cref="Id"/> is the start of its instance id,
/// <see cref="Origin"/> the text it gave (untrusted: the page escapes it), <see cref="Bricks"/> a hash of the bricks it
/// announces, which differs from a sibling's while a rolling update is under way, and <see cref="Runs"/> the runs the
/// coordinator placed on it.
/// </summary>
public sealed record NodeReplica(
    string Id, DateTimeOffset Since, string? Origin, bool Connected, string? DtpipeVersion, string Bricks,
    IReadOnlyList<string> Capabilities, int Runs);

public sealed record NodeView(
    string Name, IReadOnlyList<string> Groups, string Description, bool Online, IReadOnlyList<DatasetInfo> Datasets,
    NodeRole Role, IReadOnlyList<BrickInfo> Bricks, IReadOnlyList<string> Capabilities,
    string? DtpipeVersion, IReadOnlyList<NodeReplica> Replicas, bool ReplicasDisagree);

/// <summary>
/// The nodes the coordinator knows of, by the name their rights give them, over <see cref="ReplicaRegistry"/>, the one
/// table of the hosts present. A node is online while any of its replicas is connected. What the node announces is read from
/// its most recently arrived connected replica (the same host reconnecting does not change which); a node with none keeps
/// the announcement it last made for as long as its identity exists.
/// </summary>
public sealed class NodeInventory(ReplicaRegistry registry)
{
    private sealed class Node
    {
        /// <summary>The name the rights give the identity, as the last announcement carried it.</summary>
        public required string Name;
        public required NodeAnnouncement Announcement;
        public required IReadOnlyList<string> Groups;
    }

    private readonly object _lock = new();
    // By subject: the identity's client id, which is also the key of its replicas in the registry. A rename changes
    // the name an entry shows when the host announces under it, and never makes a second entry.
    private readonly Dictionary<string, Node> _nodes = new(StringComparer.Ordinal);

    /// <summary>Records what a replica announced, once the registry took it.</summary>
    public void Announced(string name, string subject, NodeAnnouncement announcement, IReadOnlyList<string> groups)
    {
        lock (_lock)
            _nodes[subject] = new Node { Name = name, Announcement = announcement, Groups = groups };
    }

    /// <summary>The subject of the node whose replica holds <paramref name="connectionId"/>, once the coordinator accepted its announcement.</summary>
    public string? SubjectOf(string connectionId) => registry.OfConnection(connectionId)?.Key.Node;

    /// <summary>The node whose replica holds <paramref name="connectionId"/>, by the name its rights give it.</summary>
    public string? NodeOf(string connectionId) => registry.OfConnection(connectionId)?.Announcement.Name;

    /// <summary>Drops the control connection of every replica of the node <paramref name="subject"/> carries: their hosts announce again, with a new token.</summary>
    public int Disconnect(string subject) => registry.Disconnect(subject);

    /// <summary>
    /// Drops an offline node from the inventory: its identity is gone. The replicas it has that are not connected are lost, and the
    /// runs placed on them end; a node with a connected replica stays until that one goes.
    /// </summary>
    public void Forget(string subject)
    {
        registry.Forget(subject);
        if (registry.Replicas(subject).Count > 0) return;
        lock (_lock) _nodes.Remove(subject);
    }

    /// <summary>The runner a request for the coordinator goes to: one online first, then by name; <c>null</c> when none is known.</summary>
    public NodeView? PreferredRunner() => Snapshot().Where(n => n.Role == NodeRole.Runner)
        .OrderByDescending(n => n.Online).ThenBy(n => n.Name, StringComparer.Ordinal).FirstOrDefault();

    private string? SubjectNamed(string name)
    {
        lock (_lock) return _nodes.FirstOrDefault(n => n.Value.Name == name).Key;
    }

    /// <summary>The subject a node is carried under, while a replica of it is connected: its identity's client id.</summary>
    public string? ClientIdOf(string name) =>
        SubjectNamed(name) is { } subject && registry.Replicas(subject).Any(r => r.Connected) ? subject : null;

    /// <summary>The connection a read of the node goes to: one of its connected replicas, the one with the fewest runs. Null when none is connected.</summary>
    public string? ConnectionForRead(string name) =>
        SubjectNamed(name) is { } subject ? registry.ConnectionForRead(subject) : null;

    /// <summary>The key and connection of every connected replica of the node, oldest first: what a message to the whole node goes to.</summary>
    public IReadOnlyList<(ReplicaKey Key, string ConnectionId)> ReplicasOf(string name) =>
        SubjectNamed(name) is { } subject ? registry.ConnectionsOf(subject) : [];

    public IReadOnlyList<NodeView> Snapshot()
    {
        List<KeyValuePair<string, Node>> nodes;
        lock (_lock) nodes = _nodes.OrderBy(n => n.Value.Name, StringComparer.Ordinal).ToList();

        return nodes.Select(n =>
        {
            var replicas = registry.Replicas(n.Key);
            var connected = replicas.Where(r => r.Connected).ToList();
            var reference = connected.OrderByDescending(r => r.FirstAnnouncedAt).FirstOrDefault();
            var announcement = reference?.Announcement ?? n.Value.Announcement;
            var views = replicas.Select(r => new NodeReplica(
                r.Key.Short, r.FirstAnnouncedAt, r.Announcement.Origin, r.Connected, r.Announcement.DtpipeVersion,
                BricksHash(r.Announcement.Bricks), r.Announcement.Capabilities ?? [], r.Leases.Count)).ToList();
            return new NodeView(
                n.Value.Name, reference?.Groups ?? n.Value.Groups, announcement.Description, connected.Count > 0, announcement.Datasets,
                announcement.Role, announcement.Bricks ?? [], announcement.Capabilities ?? [], announcement.DtpipeVersion, views,
                views.Where(v => v.Connected).Select(v => v.Bricks).Distinct().Count() > 1);
        }).ToList();
    }

    /// <summary>The start of a hash of the bricks a replica announces: equal on replicas that offer the same, different while one is being updated.</summary>
    private static string BricksHash(IReadOnlyList<BrickInfo>? bricks) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('\n',
            (bricks ?? []).OrderBy(b => b.Id, StringComparer.Ordinal).Select(b => $"{b.Id}\t{b.Kind}\t{b.Branch}")))))[..8];
}

/// <summary>
/// The node channel (<see cref="NodeChannel"/>), one connection per replica, each authenticated by the trusted IdP. A
/// host is who its token says, and the rights say what that is: the node name and the groups are its identity's, and the
/// name a host proposes for itself is only ever a proposal, shown to an administrator when no identity knows the token's
/// subject. The coordinator knows the caller of each message by its connection alone: what a replica reports is filed
/// under the node its connection announced, and what it says of a run is heard only if the run placed it there.
/// </summary>
public sealed class NodeChannelHub(
    NodeInventory inventory, ReplicaProtocol protocol, RightsStore rights, EnrollmentQueue enrollment, CoordinatorServerOptions options,
    EventBus bus, ILogger<NodeChannelHub> logger) : Hub
{
    public async Task Announce(NodeAnnouncement announcement)
    {
        var clientId = TrustedIdp.SubjectOf(Context.User);
        var identity = rights.Find(clientId);
        if (identity is null)
        {
            Propose(clientId, announcement);
            Context.Abort();
            return;
        }
        if (!identity.Enabled)
        {
            Refuse(identity.Name, $"'{clientId}' is disabled");
            Context.Abort();
            return;
        }
        // Where a host runs is its own word, shown to people: cut here and escaped by the page.
        var origin = announcement.Origin is null ? null : EnrollmentQueue.Clean(announcement.Origin, EnrollmentQueue.MaxOrigin);
        var named = announcement with { Name = identity.Name, Origin = origin };
        var (replica, refusal) = await protocol.OnAnnouncedAsync(Context.ConnectionId, identity.ClientId, named, identity.Groups, Context.Abort);
        if (replica is null)
        {
            Refuse(identity.Name, refusal ?? "the announcement was not accepted");
            Context.Abort();
            return;
        }
        inventory.Announced(identity.Name, identity.ClientId, named, identity.Groups);
        bus.Publish("nodes", inventory.Snapshot());
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

    /// <summary>How one run of a fragment ended on this replica. The orchestrator hears it only from the replica the run placed the fragment on.</summary>
    public Task<ExitedAck> Exited(FragmentExit exit) => protocol.OnExitedAsync(Context.ConnectionId, exit);

    /// <summary>This replica is leaving for good: its runs end now, ahead of the close this host is about to make.</summary>
    public Task Leave()
    {
        protocol.OnLeave(Context.ConnectionId);
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
        var subject = inventory.SubjectOf(Context.ConnectionId);
        protocol.OnDisconnected(Context.ConnectionId);
        // A node whose identity is gone is no longer a node of this coordinator. A rename is not that: the host
        // announces again under the new name, and the entry takes it.
        if (subject is not null && rights.Find(subject) is null) inventory.Forget(subject);
        bus.Publish("nodes", inventory.Snapshot());
        return base.OnDisconnectedAsync(exception);
    }
}
