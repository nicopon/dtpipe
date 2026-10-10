using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// Asks a node, over the node channel, to read its own data: a brick preview, a lab query, a cut. The
/// server never opens a node's database; a replica of the node that hosts it answers (the one with the fewest runs), or
/// says why it will not. A read is not tried on a second replica when the first fails: the answer says which was asked.
/// </summary>
public sealed class NodeRequests(NodeInventory inventory, IHubContext<NodeChannelHub> hub)
{
    /// <summary>The node announcing <paramref name="variable"/>, or <c>null</c> when none does.</summary>
    public string? HostOf(string variable) =>
        inventory.Snapshot().FirstOrDefault(n => n.Datasets.Any(d => d.Variable == variable))?.Name;

    /// <summary>
    /// The answer of <paramref name="node"/>'s request <paramref name="name"/>, a request a host registered under a
    /// name of its own, with <paramref name="payload"/> as JSON; <paramref name="failed"/> turns why it did not
    /// answer (offline, not registered, a handler that threw) into the answer. Throws <see cref="ConflictException"/>
    /// only when the node is offline.
    /// </summary>
    public async Task<T> RequestAsync<T>(string node, string name, object payload, TimeSpan bound, Func<string, T> failed, CancellationToken ct)
    {
        var response = await AskAsync<NodeResponse>(node, NodeChannel.Request, [name, JsonSerializer.Serialize(payload, Json)], bound, ct);
        if (response.Payload is null) return failed(response.Error ?? $"{node} sent no answer to '{name}'.");
        return JsonSerializer.Deserialize<T>(response.Payload, Json) ?? failed($"{node} answered '{name}' with nothing.");
    }

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

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
