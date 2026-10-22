using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// Asks a node, over the node channel, to read its own data: a brick preview, a sample, a check. The
/// server never opens a node's database; a replica of the node that hosts it answers (the one with the fewest runs), or
/// says why it will not. A read is not tried on a second replica when the first fails: the answer says which was asked.
/// </summary>
public sealed class NodeRequests(NodeInventory inventory, IHubContext<NodeChannelHub> hub)
{
    /// <summary>The node announcing <paramref name="variable"/>, or <c>null</c> when none does.</summary>
    public string? HostOf(string variable) =>
        inventory.Snapshot().FirstOrDefault(n => n.Datasets.Any(d => d.Variable == variable))?.Name;

    /// <summary>
    /// <paramref name="method"/>'s answer from the runner: the coordinator runs no dtpipe itself, so what
    /// needs one goes to a runner. Throws <see cref="ConflictException"/> when none is online.
    /// </summary>
    public async Task<T> AskRunnerAsync<T>(string method, object?[] args, TimeSpan bound, CancellationToken ct)
    {
        var runner = inventory.PreferredRunner();
        if (runner is not { Online: true })
            throw new ConflictException("No runner is online: the coordinator runs no dtpipe itself, and a runner does it for it.");
        return await AskAsync<T>(runner.Name, method, args, bound, ct);
    }

    /// <summary>
    /// <paramref name="method"/>'s answer from <paramref name="node"/>, within <paramref name="bound"/>.
    /// Throws <see cref="ConflictException"/> when the node is offline.
    /// </summary>
    public async Task<T> AskAsync<T>(string node, string method, object?[] args, TimeSpan bound, CancellationToken ct)
    {
        var connection = inventory.ConnectionForRead(node) ?? throw new ConflictException($"{node} is offline.");
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(bound);
        return await hub.Clients.Client(connection).InvokeCoreAsync<T>(method, args, bounded.Token);
    }
}
