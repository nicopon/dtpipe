using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using TransportR.Interfaces;

namespace DtPipe.Coordinator.Server;

/// <summary>A request the server refuses in its current state (HTTP 409).</summary>
public sealed class ConflictException(string message) : Exception(message);

/// <summary><paramref name="Replicas"/>: how many replicas of the node could run the fragment now, which is what a run needs.</summary>
public sealed record DeployedFragment(string Name, string Node, string Version, int? Replicas = null);

public sealed record DeploymentView(
    string PipelineId, string PlanId, string Origin, string State, DateTimeOffset DeployedAt, string? Message,
    IReadOnlyList<DeployedFragment> Fragments, int Edges, int Queued, string? RunningRunId);

/// <summary>
/// What the routes see of deployments and runs. A deployment is the coordinator's: the plan, where it came from, and a
/// state (<see cref="DeploymentService"/>); nothing of it is left on a node. A run is queued, sent to the orchestrator and
/// journaled (<see cref="RunQueue"/>); several pipelines may be deployed at once, their fragments told apart by name
/// (<c>&lt;pipeline&gt;@&lt;node&gt;</c>). Fault injection on a deployed fragment is the bench's (<see cref="LabFaults"/>).
/// The three share one <see cref="DeploymentLedger"/> and its lock; this class composes them and decides nothing.
/// </summary>
public sealed class DeploymentManager
{
    private readonly DeploymentService _deployments;
    private readonly RunQueue _runs;
    private readonly LabFaults _lab;

    public DeploymentManager(
        NodeInventory inventory, INodeLink link, IRunOrchestrator orchestrator, ReplicaAdmission admission,
        IFlowControlService flowControl, RightsStore rights, EventBus bus, RunJournal journal, ILogger<DeploymentManager> logger)
    {
        var ledger = new DeploymentLedger();
        _deployments = new DeploymentService(ledger, inventory, link, admission, flowControl, rights, bus, logger);
        _runs = new RunQueue(ledger, _deployments, inventory, orchestrator, bus, journal, logger);
        _lab = new LabFaults(ledger, inventory, link);
    }

    /// <summary>The plan deployed last: what the Lab view shows and what a run naming no pipeline runs.</summary>
    public DistributionPlan? ActivePlan => _deployments.ActivePlan;

    /// <summary>The oldest run in flight, else the next waiting, else the last finished: of one pipeline, or of any.</summary>
    public RunView? CurrentRun(string? pipelineId = null) => _runs.CurrentRun(pipelineId);

    /// <summary>The runs in flight, oldest first, then the waiting ones in the order they came.</summary>
    public IReadOnlyList<RunView> Queue => _runs.Queue;

    /// <summary>The deployments, each fragment with the number of replicas that could run it now.</summary>
    public Task<IReadOnlyList<DeploymentView>> DeploymentsAsync() => _deployments.DeploymentsAsync();

    /// <summary>The plan id a pipeline is deployed from, or null when it is not deployed.</summary>
    public string? DeployedPlanId(string pipelineId) => _deployments.DeployedPlanId(pipelineId);

    public DistributionPlan? PlanOf(string pipelineId) => _deployments.PlanOf(pipelineId);

    public Task DeployAsync(DistributionPlan plan, string origin, CancellationToken ct) => _deployments.DeployAsync(plan, origin, ct);

    public Task UndeployAsync(string pipelineId, CancellationToken ct) => _deployments.UndeployAsync(pipelineId, ct);

    /// <summary>Queues a run of <paramref name="pipelineId"/>, or of the plan deployed last when it is null.</summary>
    public RunView Enqueue(string? pipelineId) => _runs.Enqueue(pipelineId);

    /// <summary>
    /// Cancels a run in flight or still waiting. With no id, the one in flight: several in flight must
    /// be named, and none leaves everything as it is.
    /// </summary>
    public void Cancel(string? runId = null) => _runs.Cancel(runId);

    /// <summary>Fault injection: ends the child of the fragment's run on every replica of its node.</summary>
    public Task KillAsync(string fragmentName, CancellationToken ct) => _lab.KillAsync(fragmentName, ct);
}
