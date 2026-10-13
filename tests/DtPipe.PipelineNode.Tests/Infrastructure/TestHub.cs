using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.SignalR.Access;
using TransportR.Hub.SignalR.Configuration;
using TransportR.Hub.SignalR.Hubs;
using TransportR.Hub.SignalR.Services;
using TransportR.Interfaces;

namespace DtPipe.PipelineNode.Tests.Infrastructure;

/// <summary>
/// TransportR's own <see cref="CommandHub"/> has no usable concrete form of its own — every real
/// host, including the coordinator, derives one. This one adds nothing: these tests have no coordinator,
/// so the test harness plays that role directly through <c>ITransferInitiator</c>/<c>ITransferTerminator</c>.
/// </summary>
public sealed class TestHub : CommandHub
{
    public TestHub(
        IStateStore stateStore,
        ITransferManager transferManager,
        IClientIdentityProvider identityProvider,
        SignalRTransferInitiator initiator,
        AccessPolicy policy,
        ConnectionRegistry connections,
        IOptions<AffinityOptions> affinityOptions,
        ILogger<CommandHub> logger,
        TimeProvider timeProvider)
        : base(stateStore, transferManager, identityProvider, initiator, policy, connections, affinityOptions, logger, timeProvider)
    {
    }
}
