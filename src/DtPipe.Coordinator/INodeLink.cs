using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Coordinator;

/// <summary>A message for a replica that has no connection: it is in its grace period, or gone.</summary>
public sealed class ReplicaUnreachableException(ReplicaKey replica) : Exception($"Replica {replica} is not connected.")
{
    public ReplicaKey Replica { get; } = replica;
}

/// <summary>
/// How the coordinator speaks to a replica on the node channel: every message is an invocation the replica answers,
/// resolved to the replica's connection as it is at the moment of sending. The orchestrator writes to a node by this
/// interface only, so it never knows the hub behind it.
/// </summary>
public interface INodeLink
{
    /// <summary>Spawns the fragment's child on <paramref name="replica"/>, or says why the replica refuses to.</summary>
    Task<LaunchResult> LaunchAsync(ReplicaKey replica, LaunchRequest request, CancellationToken ct);

    /// <summary>Hands the run's child the transfer of one of its edges. False when the replica knows no such run or edge.</summary>
    Task<bool> WireAsync(ReplicaKey replica, string runId, string fragment, string alias, string transferId, CancellationToken ct);

    /// <summary>Tells the replica to abandon the run's fragment. False when it knows no such run.</summary>
    Task<bool> CancelAsync(ReplicaKey replica, string runId, string fragment, CancellationToken ct);

    /// <summary>Asks whether the replica would run the fragment, which spawns nothing.</summary>
    Task<FragmentCheck> CheckAsync(ReplicaKey replica, FragmentSpec spec, CancellationToken ct);

    /// <summary>Fault injection: ends the child of every run of <paramref name="fragment"/> on the replica. How many it ended.</summary>
    Task<int> KillAsync(ReplicaKey replica, string fragment, CancellationToken ct);
}

/// <summary><see cref="INodeLink"/> over a SignalR hub, answered by the client the connection belongs to.</summary>
/// <exception cref="ReplicaUnreachableException">The replica has no connection when the message is sent.</exception>
public sealed class HubNodeLink<THub>(IHubContext<THub> hub, ReplicaRegistry registry) : INodeLink where THub : Hub
{
    private ISingleClientProxy Proxy(ReplicaKey replica) =>
        registry.ConnectionOf(replica) is { } connection ? hub.Clients.Client(connection) : throw new ReplicaUnreachableException(replica);

    // async, so a replica that has no connection is a faulted task for the caller to handle, never an exception thrown before a task exists.
    public async Task<LaunchResult> LaunchAsync(ReplicaKey replica, LaunchRequest request, CancellationToken ct) =>
        await Proxy(replica).InvokeCoreAsync<LaunchResult>(NodeChannel.Launch, [request], ct);

    public async Task<bool> WireAsync(ReplicaKey replica, string runId, string fragment, string alias, string transferId, CancellationToken ct) =>
        await Proxy(replica).InvokeCoreAsync<bool>(NodeChannel.Wire, [runId, fragment, alias, transferId], ct);

    public async Task<bool> CancelAsync(ReplicaKey replica, string runId, string fragment, CancellationToken ct) =>
        await Proxy(replica).InvokeCoreAsync<bool>(NodeChannel.Cancel, [runId, fragment], ct);

    public async Task<FragmentCheck> CheckAsync(ReplicaKey replica, FragmentSpec spec, CancellationToken ct) =>
        await Proxy(replica).InvokeCoreAsync<FragmentCheck>(NodeChannel.Check, [spec], ct);

    public async Task<int> KillAsync(ReplicaKey replica, string fragment, CancellationToken ct) =>
        await Proxy(replica).InvokeCoreAsync<int>(NodeChannel.Kill, [fragment], ct);
}

public static class NodeLinkServiceCollectionExtensions
{
    /// <summary>Registers <see cref="INodeLink"/> over the hub that serves <see cref="NodeChannel.Path"/>.</summary>
    public static IServiceCollection AddNodeLink<THub>(this IServiceCollection services) where THub : Hub =>
        services.AddSingleton<INodeLink, HubNodeLink<THub>>();
}
