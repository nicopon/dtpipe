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
    void Register(Guid clientId, string connectionId, string fragmentName);

    /// <summary>Called from <c>OnDisconnectedAsync</c>; a no-op if the connection was never registered.</summary>
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
            _byFragment[fragmentName] = new RegisteredNode(clientId, connectionId, fragmentName);
            _fragmentByConnection[connectionId] = fragmentName;
        }
    }

    public void Unregister(string connectionId)
    {
        lock (_lock)
        {
            if (_fragmentByConnection.Remove(connectionId, out var fragmentName))
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
            return _fragmentByConnection.TryGetValue(connectionId, out var fragmentName)
                ? _byFragment[fragmentName]
                : null;
    }
}
