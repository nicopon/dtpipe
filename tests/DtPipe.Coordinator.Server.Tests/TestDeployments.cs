using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Interfaces;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>A job source that holds the jobs a test puts in it, by pipeline and commit.</summary>
internal sealed class FakeJobs : IJobSource
{
    public Dictionary<(string Pipeline, string Commit), string> Jobs { get; } = [];

    public Task<string?> JobAtAsync(string pipelineId, string commit, CancellationToken ct) =>
        Task.FromResult(Jobs.TryGetValue((pipelineId, commit), out var job) ? job : null);
}

/// <summary>Builds a <see cref="DeploymentManager"/> over the parts a test already holds, planning with the real planner.</summary>
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
        Func<BrickCatalog, NodeRequests, BindingVerifier>? verifier = null)
    {
        var bricks = new BrickCatalog(inventory);
        var requests = new NodeRequests(inventory, new NoHub());
        var planner = new PlanBuilder(inventory, requests, bricks, new PlanRegistry(flow), rights);
        return new DeploymentManager(
            inventory, link, runs, admission, flow, rights, bus, journal, new AutoPlacer(bricks, inventory, planner), bricks,
            verifier?.Invoke(bricks, requests) ?? new BindingVerifier(bricks, requests), jobs ?? new FakeJobs(), options, NullLogger<DeploymentManager>.Instance);
    }
}
