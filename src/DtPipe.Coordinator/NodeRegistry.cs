namespace DtPipe.Coordinator;

/// <summary>A pipeline node's coordinator-side identity: its TransportR client and its live connection.</summary>
public sealed record RegisteredNode(Guid ClientId, string ConnectionId, string FragmentName);

public sealed class NodeRegistryOptions
{
    /// <summary>
    /// How long a fragment stays reclaimable after its connection drops before
    /// <see cref="INodeRegistry.FragmentLost"/> fires. A SignalR connection auto-reconnects on its
    /// own default schedule (0/2/10/30s, ~42s total) before giving up, so this must clear that
    /// window or an ordinary reconnect is reported as a lost fragment.
    /// </summary>
    public TimeSpan DisconnectGracePeriod { get; init; } = TimeSpan.FromSeconds(45);
}

/// <summary>
/// The fragments currently declared by a connected pipeline node, keyed by fragment name - the
/// admission barrier's inventory. A node's <c>Connect</c> (TransportR) always precedes its
/// <c>Register</c> (coordinator); this registry only ever sees identities the state store already
/// knows.
/// </summary>
public interface INodeRegistry
{
    /// <exception cref="InvalidOperationException">
    /// <paramref name="fragmentName"/> is held by another connection that is not currently in its
    /// disconnect grace period - a genuine name collision, not a reconnect.
    /// </exception>
    void Register(Guid clientId, string connectionId, string fragmentName);

    /// <summary>
    /// Called from <c>OnDisconnectedAsync</c>. Does not drop the fragment outright: it starts the
    /// disconnect grace period, during which <see cref="Register"/> can reclaim the name from any
    /// connection - the reconnect that follows almost always carries a fresh SignalR ConnectionId,
    /// and reconnecting under a fresh ClientId (the node process itself was restarted) must reclaim
    /// just the same. Only removes the entry if it still points to this connection: a disconnect
    /// and the re-registration that replaces it race by nature, and a disconnect that loses that
    /// race must not evict the live entry it no longer owns.
    /// </summary>
    void Unregister(string connectionId);

    /// <summary>Null while a fragment is unregistered or within its disconnect grace period - not yet confirmed live.</summary>
    RegisteredNode? TryGetByFragment(string fragmentName);

    /// <summary>Null once the owning connection has disconnected, even inside the grace period: that connection id is dead.</summary>
    RegisteredNode? TryGetByConnection(string connectionId);

    /// <summary>
    /// Fires once a fragment's disconnect grace period elapses with no reclaim - the point at which
    /// a dropped connection actually becomes a lost node, distinct from the many drops that are
    /// resolved by an ordinary reconnect and never reach here.
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
    private readonly Dictionary<string, Entry> _byFragment = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _fragmentByConnection = new(StringComparer.Ordinal);
    private readonly NodeRegistryOptions _options;

    public event Action<string, Guid>? FragmentLost;

    public NodeRegistry(NodeRegistryOptions? options = null)
    {
        _options = options ?? new NodeRegistryOptions();
    }

    public void Register(Guid clientId, string connectionId, string fragmentName)
    {
        Guid? reclaimedFromClientId = null;
        lock (_lock)
        {
            if (_byFragment.TryGetValue(fragmentName, out var existing))
            {
                if (existing.Grace is null && existing.Node.ClientId != clientId)
                    throw new InvalidOperationException(
                        $"Fragment '{fragmentName}' is already registered by another live connection.");

                // Either the same node reconnected (live, same ClientId - idempotent), or this is a
                // reclaim of an entry within its grace period (any ClientId: a restarted node
                // process gets a fresh one, and that is still the same logical fragment reclaiming
                // its name). Cancelling a live entry's null Grace is a harmless no-op.
                existing.Grace?.Cancel();
                existing.Grace?.Dispose();

                // The old ConnectionId no longer owns this name - drop its index entry, or
                // TryGetByConnection on a connection that is dead (a live reclaim) or already
                // superseded (a grace-period reclaim) keeps resolving to the entry it no longer
                // owns.
                if (existing.Node.ConnectionId != connectionId)
                    _fragmentByConnection.Remove(existing.Node.ConnectionId);

                // A *different* ClientId reclaiming a still-pending entry means the original node is
                // definitively gone - a restarted process, not a reconnect of the same one - and
                // nothing else will ever fire FragmentLost for it: the grace timer this reclaim just
                // cancelled was its only path there.
                if (existing.Grace is not null && existing.Node.ClientId != clientId)
                    reclaimedFromClientId = existing.Node.ClientId;
            }

            _byFragment[fragmentName] = new Entry { Node = new RegisteredNode(clientId, connectionId, fragmentName) };
            _fragmentByConnection[connectionId] = fragmentName;
        }

        if (reclaimedFromClientId is { } lostClientId)
            FragmentLost?.Invoke(fragmentName, lostClientId);
    }

    public void Unregister(string connectionId)
    {
        string fragmentName;
        Guid clientId;
        bool declareImmediately;
        CancellationTokenSource? grace = null;
        lock (_lock)
        {
            if (!_fragmentByConnection.Remove(connectionId, out fragmentName!))
                return;

            // The entry may already belong to a different connection that re-registered this same
            // fragment name after this one dropped but before this Unregister ran - do not touch it.
            if (!_byFragment.TryGetValue(fragmentName, out var entry) || entry.Node.ConnectionId != connectionId)
                return;

            clientId = entry.Node.ClientId;
            declareImmediately = _options.DisconnectGracePeriod <= TimeSpan.Zero;
            if (declareImmediately)
            {
                _byFragment.Remove(fragmentName);
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
            if (!_byFragment.TryGetValue(fragmentName, out var current) || !ReferenceEquals(current.Grace, grace))
                return; // reclaimed between the delay elapsing and this lock
            _byFragment.Remove(fragmentName);
        }

        grace.Dispose();
        FragmentLost?.Invoke(fragmentName, clientId);
    }

    public RegisteredNode? TryGetByFragment(string fragmentName)
    {
        lock (_lock)
            return _byFragment.TryGetValue(fragmentName, out var entry) && entry.Grace is null ? entry.Node : null;
    }

    public RegisteredNode? TryGetByConnection(string connectionId)
    {
        lock (_lock)
        {
            if (!_fragmentByConnection.TryGetValue(connectionId, out var fragmentName))
                return null;
            return _byFragment.TryGetValue(fragmentName, out var entry) && entry.Grace is null ? entry.Node : null;
        }
    }
}
