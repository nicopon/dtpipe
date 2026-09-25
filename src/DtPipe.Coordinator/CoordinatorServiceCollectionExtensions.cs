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
    /// own, and the anti peer-to-peer filter. Returns TransportR's own builder, already pointed at
    /// <see cref="CoordinatorHub"/>, so the caller finishes it (<c>UseDevMode</c> or a JWT identity
    /// provider, then <c>Build()</c>) the same way any other TransportR host does.
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

        services.AddSingleton<INodeRegistry, NodeRegistry>();
        services.AddSingleton<AdmissionGate>();
        services.AddSingleton<IRunOrchestrator, RunOrchestrator>();

        return services.AddDataHub().UseHub<CoordinatorHub>();
    }
}
