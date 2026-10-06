using Microsoft.AspNetCore.SignalR;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// Asks a node, over the node channel, to read its own data: a brick preview, a lab query, a cut. The
/// server never opens a node's database; the node that hosts it answers, or says why it will not.
/// </summary>
public sealed class NodeRequests(NodeInventory inventory, IHubContext<NodeChannelHub> hub)
{
    /// <summary>The node announcing <paramref name="variable"/>, or <c>null</c> when none does.</summary>
    public string? HostOf(string variable) =>
        inventory.Snapshot().FirstOrDefault(n => n.Datasets.Any(d => d.Variable == variable))?.Name;

    /// <summary>
    /// <paramref name="method"/>'s answer from <paramref name="node"/>, within <paramref name="bound"/>.
    /// Throws <see cref="ConflictException"/> when the node is offline.
    /// </summary>
    public async Task<T> AskAsync<T>(string node, string method, object?[] args, TimeSpan bound, CancellationToken ct)
    {
        var connection = inventory.ConnectionOf(node) ?? throw new ConflictException($"{node} is offline.");
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(bound);
        return await hub.Clients.Client(connection).InvokeCoreAsync<T>(method, args, bounded.Token);
    }
}
