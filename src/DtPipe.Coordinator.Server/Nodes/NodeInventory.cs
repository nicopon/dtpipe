using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server.Access;
using DtPipe.Coordinator.Server.Runs;
using Microsoft.AspNetCore.SignalR;

namespace DtPipe.Coordinator.Server.Nodes;

/// <summary>
/// One host carrying a node's identity, as a page shows it. <see cref="Id"/> is the start of its instance id,
/// <see cref="Origin"/> the text it gave (untrusted: the page escapes it), <see cref="Bricks"/> a hash of the bricks it
/// announces, which differs from a sibling's while a rolling update is under way, and <see cref="Runs"/> the runs the
/// coordinator placed on it. While its connection is dropped, <see cref="AwaySince"/> is when it dropped and
/// <see cref="GivenUpAt"/> when the coordinator stops waiting for its host to announce again; both are null otherwise.
/// <see cref="DataPlaneProtection"/> and <see cref="KeyId"/> are what the host announced of its data plane, as the coordinator reads
/// them (<see cref="DataPlaneProtections"/>): a word it knows, and the fingerprint of a pre-shared key, never the key.
/// <see cref="BelowRequiredProtection"/> says its level is under the one the deployment requires (<see cref="NodeView.RequiredProtection"/>):
/// the replica is connected and listed all the same, and is eligible for no fragment.
/// </summary>
public sealed record NodeReplica(
    string Id, DateTimeOffset Since, string? Origin, bool Connected, string? DtpipeVersion, string Bricks,
    IReadOnlyList<string> Capabilities, int Runs, DateTimeOffset? AwaySince = null, DateTimeOffset? GivenUpAt = null,
    string DataPlaneProtection = DataPlaneProtections.None, string? KeyId = null, bool BelowRequiredProtection = false);

/// <summary>
/// A node as a page shows it. <see cref="ReplicasDisagree"/> says two connected replicas offer different bricks, and
/// <see cref="ReplicasDisagreeOnProtection"/> that they announce a different protection of the data plane (level or key id): both
/// are what a rolling update looks like, and neither refuses a replica. <see cref="RequiredProtection"/> is the level of protection of
/// the data plane the deployment requires of every replica, a word of <see cref="DataPlaneProtections"/>: the weakest, which requires
/// nothing, unless the coordinator is set otherwise.
/// </summary>
public sealed record NodeView(
    string Name, IReadOnlyList<string> Groups, string Description, bool Online, IReadOnlyList<DatasetInfo> Datasets,
    NodeRole Role, IReadOnlyList<BrickInfo> Bricks, IReadOnlyList<string> Capabilities,
    string? DtpipeVersion, IReadOnlyList<NodeReplica> Replicas, bool ReplicasDisagree, bool ReplicasDisagreeOnProtection = false,
    string RequiredProtection = DataPlaneProtections.None);

/// <summary>
/// The nodes the coordinator knows of, by the name their rights give them, over <see cref="ReplicaRegistry"/>, the one
/// table of the hosts present. A node is online while any of its replicas is connected. What the node announces is read from
/// its most recently arrived connected replica (the same host reconnecting does not change which); a node with none keeps
/// the announcement it last made for as long as its identity exists. <paramref name="admission"/> holds the protection the deployment
/// requires, which admission reads too, so that a replica shown as under it is one admission treats so.
/// </summary>
public sealed class NodeInventory(ReplicaRegistry registry, AdmissionOptions? admission = null)
{
    private readonly string _required = (admission ?? new AdmissionOptions()).RequiredProtection;

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
    // The runs whose requester asked for a cancellation and that have not ended: a view of what is being wound down, never a verdict.
    private readonly HashSet<string> _cancelRequested = new(StringComparer.Ordinal);
    // By subject: the difference of protection between a node's replicas that the log told last, so the same one is told once.
    private readonly Dictionary<string, string> _protectionDisagreements = new(StringComparer.Ordinal);
    // By replica: the level the log told last of a replica under the required protection, so that the same one is told once.
    private readonly Dictionary<ReplicaKey, string> _protectionShortfalls = [];

    /// <summary>Records that the requester of run <paramref name="runId"/> asked for its cancellation.</summary>
    public void CancelRequested(string runId)
    {
        lock (_lock) _cancelRequested.Add(runId);
    }

    /// <summary>Forgets the cancellation request of run <paramref name="runId"/>, which has ended.</summary>
    public void CancelSettled(string runId)
    {
        lock (_lock) _cancelRequested.Remove(runId);
    }

    /// <summary>
    /// Whether the replica behind <paramref name="connectionId"/> holds runs and the requester of every one of them asked for its
    /// cancellation: what it logs now belongs to that teardown. A replica that also holds a run nobody asked to cancel is not winding down.
    /// </summary>
    public bool IsWindingDown(string connectionId)
    {
        if (registry.OfConnection(connectionId) is not { Leases.Count: > 0 } replica) return false;
        lock (_lock) return replica.Leases.All(l => _cancelRequested.Contains(l.RunId));
    }

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
        lock (_lock)
        {
            _nodes.Remove(subject);
            _protectionDisagreements.Remove(subject);
        }
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
                BricksHash(r.Announcement.Bricks), r.Announcement.Capabilities ?? [], r.Leases.Count,
                r.AwaySince, r.AwaySince + registry.DisconnectGrace, r.DataPlaneProtection, r.KeyId,
                ProtectionPolicy.IsBelow(r, _required))).ToList();
            return new NodeView(
                n.Value.Name, reference?.Groups ?? n.Value.Groups, announcement.Description, connected.Count > 0, announcement.Datasets,
                announcement.Role, announcement.Bricks ?? [], announcement.Capabilities ?? [], announcement.DtpipeVersion, views,
                views.Where(v => v.Connected).Select(v => v.Bricks).Distinct().Count() > 1, ProtectionsOf(connected).Count > 1, _required);
        }).ToList();
    }

    // A replica in grace is left out: only the hosts connected now are set one way or another.
    private static IReadOnlyList<string> ProtectionsOf(IEnumerable<ReplicaView> connected) =>
        [.. connected.Select(r => r.KeyId is null ? r.DataPlaneProtection : $"{r.DataPlaneProtection} (key {r.KeyId})")
            .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)];

    /// <summary>
    /// What the connected replicas of the node <paramref name="subject"/> announce of their data plane's protection when it is not the
    /// same on all of them, worded for the log (the levels, and the key ids), and only the first time that set is seen: a replica that
    /// announces again, as every reconnection does, says nothing new. Null while they agree, which also forgets the last difference,
    /// so a later one is told again. Nothing is refused on it: a rolling update passes through a node whose replicas differ.
    /// </summary>
    public string? NewProtectionDisagreement(string subject)
    {
        var protections = ProtectionsOf(registry.Replicas(subject).Where(r => r.Connected));
        var difference = protections.Count > 1 ? string.Join(", ", protections) : null;
        lock (_lock)
        {
            if (difference is null) _protectionDisagreements.Remove(subject);
            else if (!_protectionDisagreements.TryGetValue(subject, out var told) || told != difference)
            {
                _protectionDisagreements[subject] = difference;
                return difference;
            }
        }
        return null;
    }

    /// <summary>
    /// What the replica <paramref name="key"/> does and what the deployment requires, worded for the log (<see cref="ProtectionPolicy.Shortfall"/>),
    /// when its level is below the required one and that level has not been told for it yet: a replica that announces again, as every
    /// reconnection does, says nothing new, and one that announces another level below the required one is told again. Null while it
    /// is at the required level, which also forgets what was told of it, so that a later fall is told again. Nothing is refused on it:
    /// the replica stays connected and listed, and admission leaves it out of every fragment.
    /// </summary>
    public string? NewProtectionShortfall(ReplicaKey key)
    {
        var replicas = registry.Replicas();
        var replica = replicas.FirstOrDefault(r => r.Key == key);
        lock (_lock)
        {
            // A replica that is gone takes what was told of it away, as the registry does with it.
            var present = replicas.Select(r => r.Key).ToHashSet();
            foreach (var gone in _protectionShortfalls.Keys.Where(k => !present.Contains(k)).ToList()) _protectionShortfalls.Remove(gone);
            if (replica is null || !ProtectionPolicy.IsBelow(replica, _required))
            {
                _protectionShortfalls.Remove(key);
                return null;
            }
            if (_protectionShortfalls.TryGetValue(key, out var told) && told == replica.DataPlaneProtection) return null;
            _protectionShortfalls[key] = replica.DataPlaneProtection;
        }
        return ProtectionPolicy.Shortfall(replica, _required);
    }

    /// <summary>The start of a hash of the bricks a replica announces: equal on replicas that offer the same, different while one is being updated.</summary>
    private static string BricksHash(IReadOnlyList<BrickInfo>? bricks) =>
        ContentHash.Of(string.Join('\n', (bricks ?? []).OrderBy(b => b.Id, StringComparer.Ordinal).Select(b => $"{b.Id}\t{b.Kind}\t{b.Version}")), 8);
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
    EventBus bus, OrphanRuns orphans, ILogger<NodeChannelHub> logger) : Hub
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
        // A service of another kind is no host: it is known, so it never waits for approval, and it is let in nowhere here.
        if (!identity.IsNode)
        {
            Refuse(identity.Name, $"'{clientId}' is a {identity.Kind}, not a node");
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
        // Replicas set differently are told, never refused: a rolling update passes through that, and each replica stays admitted.
        if (inventory.NewProtectionDisagreement(identity.ClientId) is { } differing)
        {
            logger.LogWarning("The replicas of {Node} announce a different protection of the data plane: {Protections}", identity.Name, differing);
            bus.Publish("log", new NodeLogLine("coordinator", null, "Warning",
                $"The replicas of {identity.Name} announce a different protection of the data plane: {differing}. Every one stays admitted; a transfer between ends set differently fails."));
        }
        // A replica under the protection the deployment requires is told the same way: it stays connected and listed, and admission leaves it out of every fragment.
        if (inventory.NewProtectionShortfall(replica.Value) is { } shortfall)
        {
            logger.LogWarning("The replica {Replica} of {Node} {Shortfall}: it stays connected and listed, and no fragment is placed on it", replica.Value.Short, identity.Name, shortfall);
            bus.Publish("log", new NodeLogLine("coordinator", null, "Warning",
                $"The replica {replica.Value.Short} of {identity.Name} {shortfall}: it stays connected and listed, and no fragment is placed on it."));
        }
        bus.Publish("nodes", inventory.Snapshot());
        if (named.DataPlaneClientId is not null) orphans.OnAnnounced(replica.Value, identity.Name);
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

    /// <summary>How one run of a fragment ended on this replica. The orchestrator hears it only from the replica the run placed the fragment on, under the token of the launch that started the child.</summary>
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
        bus.Publish("log", Classified(line with { Node = node }, inventory.IsWindingDown(Context.ConnectionId)));
        return Task.CompletedTask;
    }

    /// <summary>
    /// A warning or an error of the transport library, logged by a replica whose runs are all being cancelled at their requester's
    /// asking, is how a transfer ends under that cancellation: it is shown as information and says so. The category decides, never the
    /// message; the line is kept, and no run, branch or cancellation verdict reads it.
    /// </summary>
    internal static NodeLogLine Classified(NodeLogLine line, bool windingDown) =>
        windingDown && LogCategories.IsTransport(line.Category) && line.Level is "Warning" or "Error" or "Critical"
            ? line with { Level = "Information", Message = $"{line.Message} (a consequence of stopping the run)" }
            : line;

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
