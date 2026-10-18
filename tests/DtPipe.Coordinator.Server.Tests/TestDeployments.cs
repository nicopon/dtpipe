using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Interfaces;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>A job source that holds the jobs a test puts in it, by pipeline and commit.</summary>
internal sealed class FakeJobs : IJobSource
{
    public Dictionary<(string Pipeline, string Commit), string> Jobs { get; } = [];

    /// <summary>When set, every read fails with it: the library cannot be reached.</summary>
    public Exception? Fault { get; set; }

    public Task<string?> JobAtAsync(string pipelineId, string commit, CancellationToken ct) =>
        Fault is { } fault ? Task.FromException<string?>(fault) : Task.FromResult(Jobs.TryGetValue((pipelineId, commit), out var job) ? job : null);
}

/// <summary>Builds a <see cref="DeploymentManager"/> over the parts a test already holds, planning with the real planner; its store is the options' state directory's, unless given.</summary>
internal static class TestDeployments
{
    private sealed class NoHub : IHubContext<NodeChannelHub>
    {
        public IHubClients Clients => throw new NotSupportedException();
        public IGroupManager Groups => throw new NotSupportedException();
    }

    public static DeploymentManager Create(
        NodeInventory inventory, INodeLink link, IRunOrchestrator runs, ReplicaAdmission admission, IFlowControlService flow,
        RightsStore rights, EventBus bus, RunJournal journal, CoordinatorServerOptions options, IJobSource? jobs = null,
        Func<BrickCatalog, NodeRequests, BindingVerifier>? verifier = null, IDeploymentStore? store = null)
    {
        var bricks = new BrickCatalog(inventory);
        var requests = new NodeRequests(inventory, new NoHub());
        var planner = new PlanBuilder(inventory, requests, bricks, new PlanRegistry(flow), rights);
        return new DeploymentManager(
            inventory, link, runs, admission, flow, rights, bus, journal, new AutoPlacer(bricks, inventory, planner, rights), bricks,
            verifier?.Invoke(bricks, requests) ?? new BindingVerifier(bricks, requests), jobs ?? new FakeJobs(), store ?? MemoryDeploymentStore.At(options.StateDir), NullLogger<DeploymentManager>.Instance);
    }
}
