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
    public CoordinatorHub(
        IStateStore stateStore,
        ITransferManager transferManager,
        IClientIdentityProvider identityProvider,
        IBridgeManager bridgeManager,
        SignalRTransferInitiator initiator,
        IOptions<AffinityOptions> affinityOptions,
        ILogger<CommandHub> logger,
        IFlowControlService? flowControlService = null)
        : base(stateStore, transferManager, identityProvider, bridgeManager, initiator, affinityOptions, logger, flowControlService)
    {
    }
}
