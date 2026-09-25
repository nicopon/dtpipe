namespace DtPipe.Coordinator;

/// <summary>A pipeline node's coordinator-side identity: its TransportR client and its live connection.</summary>
public sealed record RegisteredNode(Guid ClientId, string ConnectionId, string FragmentName);

/// <summary>
/// The fragments currently declared by a connected pipeline node, keyed by fragment name - the
/// admission barrier's inventory. A node's <c>Connect</c> (TransportR) always precedes its
/// <c>Register</c> (coordinator); this registry only ever sees identities the state store already
/// knows.
/// </summary>
public interface INodeRegistry
{
    /// <exception cref="InvalidOperationException"><paramref name="fragmentName"/> is already held by another live connection.</exception>
    void Register(Guid clientId, string connectionId, string fragmentName);

    /// <summary>
    /// Called from <c>OnDisconnectedAsync</c>. Only removes the fragment entry if it still points to
    /// this connection: a disconnect and the re-registration that replaces it race by nature, and a
    /// disconnect that loses that race must not drop the live entry it no longer owns.
    /// </summary>
    void Unregister(string connectionId);

    RegisteredNode? TryGetByFragment(string fragmentName);

    RegisteredNode? TryGetByConnection(string connectionId);
}

public sealed class NodeRegistry : INodeRegistry
{
    private readonly object _lock = new();
    private readonly Dictionary<string, RegisteredNode> _byFragment = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _fragmentByConnection = new(StringComparer.Ordinal);

    public void Register(Guid clientId, string connectionId, string fragmentName)
    {
        lock (_lock)
        {
            if (_byFragment.TryGetValue(fragmentName, out var existing) && existing.ConnectionId != connectionId)
                throw new InvalidOperationException(
                    $"Fragment '{fragmentName}' is already registered by another live connection.");

            _byFragment[fragmentName] = new RegisteredNode(clientId, connectionId, fragmentName);
            _fragmentByConnection[connectionId] = fragmentName;
        }
    }

    public void Unregister(string connectionId)
    {
        lock (_lock)
        {
            if (!_fragmentByConnection.Remove(connectionId, out var fragmentName))
                return;

            // The entry may already belong to a different connection that re-registered this same
            // fragment name after this one dropped but before this Unregister ran - do not remove it.
            if (_byFragment.TryGetValue(fragmentName, out var node) && node.ConnectionId == connectionId)
                _byFragment.Remove(fragmentName);
        }
    }

    public RegisteredNode? TryGetByFragment(string fragmentName)
    {
        lock (_lock)
            return _byFragment.TryGetValue(fragmentName, out var node) ? node : null;
    }

    public RegisteredNode? TryGetByConnection(string connectionId)
    {
        lock (_lock)
        {
            if (!_fragmentByConnection.TryGetValue(connectionId, out var fragmentName))
                return null;
            return _byFragment.TryGetValue(fragmentName, out var node) ? node : null;
        }
    }
}
