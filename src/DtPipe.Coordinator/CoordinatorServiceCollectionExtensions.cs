using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using TransportR.FlowControl;
using TransportR.Hub.SignalR;
using TransportR.Interfaces;

namespace DtPipe.Coordinator;

public static class CoordinatorServiceCollectionExtensions
{
    /// <summary>
    /// Registers the coordinator's share of hosting the hub: the flow control matrix backing
    /// <see cref="IPlanRegistry"/>, the progress monitor <c>AddDataHub()</c> does not provide on its
    /// own, the anti peer-to-peer filter, and the replica registry, admission and orchestrator that drive runs
    /// over the host's node channel (<see cref="NodeLinkServiceCollectionExtensions.AddNodeLink{THub}"/> names
    /// that hub; <see cref="ReplicaProtocol"/> is what its methods call). Returns TransportR's own builder, so the
    /// caller finishes it (<c>UseDevMode</c> or a JWT identity provider, then <c>Build()</c>) the same way any
    /// other TransportR host does: the TransportR hub carries the data plane and nothing else.
    /// </summary>
    public static DataHubBuilder AddCoordinatorHub(
        this IServiceCollection services, Action<FlowControlOptions> configureFlowControl)
    {
        var flowControlOptions = new FlowControlOptions();
        configureFlowControl(flowControlOptions);
        services.AddSingleton(flowControlOptions);
        services.AddSingleton<IFlowControlService, DefaultFlowControlService>();
        services.AddSingleton<IPlanRegistry, PlanRegistry>();

        services.AddSingleton<IHubProgressMonitor, LoggingHubProgressMonitor>();
        services.AddSignalR(options => options.AddFilter<PeerToPeerHubFilter>());

        services.AddSingleton<ReplicaRegistry>();
        services.AddSingleton<ReplicaAdmission>();
        services.AddSingleton<IRunOrchestrator, RunOrchestrator>();
        services.AddSingleton<ReplicaProtocol>();

        return services.AddDataHub();
    }
}
