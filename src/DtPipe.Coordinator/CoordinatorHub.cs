using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.SignalR.Configuration;
using TransportR.Hub.SignalR.Hubs;
using TransportR.Hub.SignalR.Services;
using TransportR.Interfaces;

namespace DtPipe.Coordinator;

/// <summary>
/// The single connection a pipeline node holds to the coordinator: TransportR's
/// <see cref="CommandHub"/>, carrying whatever coordinator-specific methods are added to it.
/// </summary>
public class CoordinatorHub : CommandHub
{
    private readonly IStateStore _stateStore;
    private readonly INodeRegistry _nodeRegistry;
    private readonly IRunOrchestrator _runOrchestrator;

    public CoordinatorHub(
        IStateStore stateStore,
        ITransferManager transferManager,
        IClientIdentityProvider identityProvider,
        IBridgeManager bridgeManager,
        SignalRTransferInitiator initiator,
        IOptions<AffinityOptions> affinityOptions,
        ILogger<CommandHub> logger,
        INodeRegistry nodeRegistry,
        IRunOrchestrator runOrchestrator,
        IFlowControlService? flowControlService = null)
        : base(stateStore, transferManager, identityProvider, bridgeManager, initiator, affinityOptions, logger, flowControlService)
    {
        _stateStore = stateStore;
        _nodeRegistry = nodeRegistry;
        _runOrchestrator = runOrchestrator;
    }

    // Drops only the fragment inventory entry. A run still waiting on this fragment's Ready or
    // Exited is not failed here: TransportR's own state store keeps the ClientId registration past
    // a disconnect, since a client reconnects under it, and RunOrchestrator does not yet re-route a
    // reconnected fragment to its new connection id.
    public override Task OnDisconnectedAsync(Exception? exception)
    {
        _nodeRegistry.Unregister(Context.ConnectionId);
        return base.OnDisconnectedAsync(exception);
    }

    /// <summary>
    /// Declares this connection's fragment - the admission barrier's inventory entry. The caller's
    /// identity comes from its TransportR registration (<see cref="IStateStore"/>), never from an
    /// argument: <c>Connect</c> always precedes <c>Register</c> on the single connection a node
    /// holds.
    /// </summary>
    public async Task Register(string fragmentName)
    {
        var client = await _stateStore.GetClientByConnectionIdAsync(Context.ConnectionId)
            ?? throw new HubException("Register: call Connect first.");
        _nodeRegistry.Register(client.ClientId, Context.ConnectionId, fragmentName);
    }

    /// <summary>The fragment's child process is spawned and its declared edges can be wired.</summary>
    public Task Ready(string runId) => _runOrchestrator.OnReadyAsync(runId, ResolveFragment());

    /// <summary>The fragment's own process has finished; carries what the run's outcome is built from.</summary>
    public Task Exited(string runId, int exitCode, string origin, string? firstFault, Dictionary<string, long> rowCounts) =>
        _runOrchestrator.OnExitedAsync(runId, ResolveFragment(), exitCode, origin, firstFault, rowCounts);

    private string ResolveFragment() =>
        _nodeRegistry.TryGetByConnection(Context.ConnectionId)?.FragmentName
            ?? throw new HubException("Call Register first.");
}
