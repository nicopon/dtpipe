namespace DtPipe.Coordinator;

/// <summary>
/// A pipeline node's coordinator-side identity: its TransportR client, its live connection, and the
/// version of the fragment it hosts - the hash of that fragment's own job YAML
/// (<c>PipelineNode</c>'s own version helper), never the contract hash a run's edges are checked
/// against (a different, out-of-scope concept here).
/// </summary>
public sealed record RegisteredNode(Guid ClientId, string ConnectionId, string FragmentName, string Version);

public sealed class NodeRegistryOptions
{
    /// <summary>
    /// How long an instance stays reclaimable after its connection drops before
    /// <see cref="INodeRegistry.FragmentLost"/> fires for it. A SignalR connection auto-reconnects on
    /// its own default schedule (0/2/10/30s, ~42s total) before giving up, so this must clear that
    /// window or an ordinary reconnect is reported as a lost instance.
    /// </summary>
    public TimeSpan DisconnectGracePeriod { get; init; } = TimeSpan.FromSeconds(45);
}

/// <summary>
/// The fragment instances currently declared by connected pipeline nodes - the admission barrier's
/// inventory. More than one live instance can share a fragment name at once (redundancy, horizontal
/// scaling); each is tracked independently, keyed by (fragment name, ClientId), never collapsed into
/// one entry per name. A node's <c>Connect</c> (TransportR) always precedes its <c>Register</c>
/// (coordinator); this registry only ever sees identities the state store already knows.
/// </summary>
public interface INodeRegistry
{
    /// <summary>
    /// Registers or re-registers one instance of a fragment. Never throws for a name collision: a
    /// previously-unseen <paramref name="clientId"/> registering an already-live fragment name is
    /// simply an additional instance, not a conflict - whether every live instance of one name agrees
    /// on <paramref name="version"/> is instance alignment, checked at admission
    /// (<c>AdmissionGate.Resolve</c>), never here. An (fragmentName, clientId) pair already live or
    /// within its own grace period is a reconnect: its ConnectionId and Version are updated in place
    /// and its grace, if any, is cancelled.
    /// </summary>
    void Register(Guid clientId, string connectionId, string fragmentName, string version);

    /// <summary>
    /// Called from <c>OnDisconnectedAsync</c>. Does not drop the instance outright: it starts that
    /// instance's own disconnect grace period, during which <see cref="Register"/> from the very same
    /// (fragmentName, ClientId) pair reclaims it in place - a fresh SignalR ConnectionId almost always
    /// follows, and a fresh ClientId too when the node process itself restarted. Only removes the
    /// entry if it still points to this connection: a disconnect and the re-registration that
    /// replaces it race by nature, and a disconnect that loses that race must not evict the live entry
    /// it no longer owns.
    /// </summary>
    void Unregister(string connectionId);

    /// <summary>Every currently-connected instance of a fragment name - excludes one mid-grace-period after a drop.</summary>
    IReadOnlyList<RegisteredNode> GetLiveInstances(string fragmentName);

    /// <summary>Null once the owning connection has disconnected, even inside the grace period: that connection id is dead.</summary>
    RegisteredNode? TryGetByConnection(string connectionId);

    /// <summary>
    /// True once some instance of <paramref name="fragmentName"/> has ever registered
    /// <paramref name="version"/> - retained even past every instance hosting it disconnecting past
    /// grace, since pruning old versions is a deferred product decision, not this registry's job. Lets
    /// admission tell a version that never existed (Docker's own "unknown tag") apart from one that
    /// existed but nothing hosts anymore (retired) - the free "rollback" is pinning to a version that
    /// still, or again, has a live instance.
    /// </summary>
    bool HasKnownVersion(string fragmentName, string version);

    /// <summary>
    /// Fires once an instance's disconnect grace period elapses with no reclaim - the point at which a
    /// dropped connection actually becomes a lost instance, distinct from the many drops resolved by
    /// an ordinary reconnect and never reaching here.
    /// </summary>
    event Action<string, Guid>? FragmentLost;
}

public sealed class NodeRegistry : INodeRegistry
{
    private sealed class Entry
    {
        public required RegisteredNode Node;
        /// <summary>Non-null while the owning connection has disconnected and the grace period has not yet elapsed or been cancelled by a reclaim.</summary>
        public CancellationTokenSource? Grace;
    }

    private readonly object _lock = new();

    // fragmentName -> ClientId -> Entry: one entry per live-or-graced instance, never one per
    // fragment name - several ClientIds legitimately host the same name at once (redundancy,
    // horizontal scaling), which is exactly what instance alignment checks.
    private readonly Dictionary<string, Dictionary<Guid, Entry>> _byFragment = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (string FragmentName, Guid ClientId)> _byConnection = new(StringComparer.Ordinal);

    // Every version ever registered per fragment name, appended to and never pruned - retention is a
    // deferred product decision (root CLAUDE.md's split-pipeline notes), not this registry's job.
    private readonly Dictionary<string, HashSet<string>> _knownVersions = new(StringComparer.Ordinal);

    private readonly NodeRegistryOptions _options;

    public event Action<string, Guid>? FragmentLost;

    public NodeRegistry(NodeRegistryOptions? options = null)
    {
        _options = options ?? new NodeRegistryOptions();
    }

    public void Register(Guid clientId, string connectionId, string fragmentName, string version)
    {
        lock (_lock)
        {
            if (!_byFragment.TryGetValue(fragmentName, out var instances))
            {
                instances = new Dictionary<Guid, Entry>();
                _byFragment[fragmentName] = instances;
            }

            if (!_knownVersions.TryGetValue(fragmentName, out var versions))
            {
                versions = new HashSet<string>(StringComparer.Ordinal);
                _knownVersions[fragmentName] = versions;
            }
            versions.Add(version);

            if (instances.TryGetValue(clientId, out var existing))
            {
                // Reconnect (or an idempotent re-Register) of this exact instance: cancel any pending
                // grace and update in place. A live entry's own ClientId reclaiming under a new
                // ConnectionId is always allowed too - the reconnect's Register can land before that
                // same connection's own OnDisconnectedAsync has run, so Grace is often already null
                // here and cancelling it is a harmless no-op.
                existing.Grace?.Cancel();
                existing.Grace?.Dispose();
                existing.Grace = null;

                if (existing.Node.ConnectionId != connectionId)
                    _byConnection.Remove(existing.Node.ConnectionId);

                existing.Node = new RegisteredNode(clientId, connectionId, fragmentName, version);
            }
            else
            {
                // A previously-unseen ClientId for this fragment name is simply a new, additional
                // instance - never a collision. There is no "reclaim a still-graced entry from a
                // different ClientId" case to special-case anymore: that graced entry lives under its
                // own ClientId key and this insert cannot touch it, so a restarted node's old identity
                // just times out on its own grace period like any other drop, while its replacement
                // (if it gets a fresh ClientId) registers as an ordinary new instance here.
                instances[clientId] = new Entry { Node = new RegisteredNode(clientId, connectionId, fragmentName, version) };
            }

            _byConnection[connectionId] = (fragmentName, clientId);
        }
    }

    public void Unregister(string connectionId)
    {
        string fragmentName;
        Guid clientId;
        bool declareImmediately;
        CancellationTokenSource? grace = null;
        lock (_lock)
        {
            if (!_byConnection.Remove(connectionId, out var owner))
                return;
            (fragmentName, clientId) = owner;

            // The entry may already belong to a different connection that re-registered this same
            // (fragmentName, ClientId) pair after this one dropped but before this Unregister ran -
            // do not touch it.
            if (!_byFragment.TryGetValue(fragmentName, out var instances)
                || !instances.TryGetValue(clientId, out var entry)
                || entry.Node.ConnectionId != connectionId)
                return;

            declareImmediately = _options.DisconnectGracePeriod <= TimeSpan.Zero;
            if (declareImmediately)
            {
                instances.Remove(clientId);
                if (instances.Count == 0) _byFragment.Remove(fragmentName);
            }
            else
            {
                grace = new CancellationTokenSource();
                entry.Grace = grace;
            }
        }

        if (declareImmediately)
            FragmentLost?.Invoke(fragmentName, clientId);
        else
            _ = DeclareLostAfterGraceAsync(fragmentName, clientId, grace!);
    }

    private async Task DeclareLostAfterGraceAsync(string fragmentName, Guid clientId, CancellationTokenSource grace)
    {
        try { await Task.Delay(_options.DisconnectGracePeriod, grace.Token); }
        catch (OperationCanceledException) { return; } // reclaimed - Register already cancelled and disposed this source

        lock (_lock)
        {
            if (!_byFragment.TryGetValue(fragmentName, out var instances)
                || !instances.TryGetValue(clientId, out var current)
                || !ReferenceEquals(current.Grace, grace))
                return; // reclaimed between the delay elapsing and this lock

            instances.Remove(clientId);
            if (instances.Count == 0) _byFragment.Remove(fragmentName);
        }

        grace.Dispose();
        FragmentLost?.Invoke(fragmentName, clientId);
    }

    public IReadOnlyList<RegisteredNode> GetLiveInstances(string fragmentName)
    {
        lock (_lock)
        {
            if (!_byFragment.TryGetValue(fragmentName, out var instances))
                return [];
            return instances.Values.Where(e => e.Grace is null).Select(e => e.Node).ToList();
        }
    }

    public RegisteredNode? TryGetByConnection(string connectionId)
    {
        lock (_lock)
        {
            if (!_byConnection.TryGetValue(connectionId, out var owner))
                return null;
            if (!_byFragment.TryGetValue(owner.FragmentName, out var instances)
                || !instances.TryGetValue(owner.ClientId, out var entry) || entry.Grace is not null)
                return null;
            return entry.Node;
        }
    }

    public bool HasKnownVersion(string fragmentName, string version)
    {
        lock (_lock)
            return _knownVersions.TryGetValue(fragmentName, out var versions) && versions.Contains(version);
    }
}
