using System.Text.Json.Serialization;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using TransportR.Interfaces;

namespace DtPipe.Coordinator.Server;

/// <summary>A request the server refuses in its current state (HTTP 409).</summary>
public sealed class ConflictException(string message) : Exception(message);

/// <summary><paramref name="Replicas"/>: how many replicas of the node could run the fragment now, which is what a run needs.</summary>
public sealed record DeployedFragment(string Name, string Node, string Version, int? Replicas = null);

public sealed record DeploymentView(
    string PipelineId, string PlanId, string Origin, [property: JsonConverter(typeof(WireEnumConverter<DeploymentState>))] DeploymentState State,
    DateTimeOffset DeployedAt, string? Message,
    IReadOnlyList<DeployedFragment> Fragments, int Edges, int Queued, string? RunningRunId,
    string? Commit = null, IReadOnlyList<string>? Nodes = null);

/// <summary>
/// What the routes see of deployments and runs. A deployment is the coordinator's: a library pipeline armed at a commit, or a
/// plan the Lab view fixed, where it came from, and a state (<see cref="DeploymentService"/>); nothing of it is left on a node. A run is queued, sent to the orchestrator and
/// journaled (<see cref="RunQueue"/>); several pipelines may be deployed at once, their fragments told apart by name
/// (<c>&lt;pipeline&gt;@&lt;node&gt;</c>). Fault injection on a deployed fragment is the bench's (<see cref="LabFaults"/>).
/// The three share one <see cref="DeploymentLedger"/> and its lock; this class composes them and decides nothing.
/// </summary>
public sealed class DeploymentManager
{
    private readonly DeploymentService _deployments;
    private readonly RunQueue _runs;
    private readonly LabFaults _lab;
    private readonly LaunchPlanner _planner;

    public DeploymentManager(
        NodeInventory inventory, INodeLink link, IRunOrchestrator orchestrator, ReplicaAdmission admission,
        IFlowControlService flowControl, RightsStore rights, EventBus bus, RunJournal journal,
        AutoPlacer placer, BrickCatalog bricks, BindingVerifier verifier, IJobSource jobs, CoordinatorServerOptions options, ILogger<DeploymentManager> logger)
    {
        var ledger = new DeploymentLedger();
        var planner = new LaunchPlanner(placer, bricks, inventory, admission, rights);
        _planner = planner;
        _deployments = new DeploymentService(
            ledger, inventory, link, admission, new PlanRegistry(flowControl), planner, verifier, new DeploymentStore(options.StateDir, logger), jobs, bus, logger);
        _runs = new RunQueue(ledger, _deployments, planner, inventory, orchestrator, bus, journal, logger);
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

    /// <summary>The plan a launch of <paramref name="yaml"/> under <paramref name="bindings"/> would make now, runner included. Nothing is kept.</summary>
    public Task<Launch> PlanLaunchAsync(string pipelineId, string yaml, IReadOnlyDictionary<string, DtPipe.Jobs.Coordinator.Binding> bindings, CancellationToken ct) =>
        _planner.ChooseAsync(pipelineId, yaml, bindings, ct);

    /// <summary>Every library pipeline that is armed, with where its deployment stands.</summary>
    public IReadOnlyList<(ArmedDeployment Armed, DeploymentState State, string? Message)> Armed() => _deployments.ArmedDeployments();

    /// <summary>For each fragment of <paramref name="plan"/>, the replicas that could run it now; null when a node of the plan has no usable identity. Reserves nothing.</summary>
    public Task<IReadOnlyDictionary<string, FragmentEligibility>?> EligibilityOfAsync(DistributionPlan plan) => _planner.EligibilityOfAsync(plan);

    /// <summary>What a library pipeline is armed with, or null when it is not armed.</summary>
    public ArmedDeployment? ArmedOf(string pipelineId) => _deployments.ArmedOf(pipelineId);

    /// <summary>Deploys a plan the Lab view fixed.</summary>
    public Task DeployAsync(DistributionPlan plan, string origin, CancellationToken ct) => _deployments.DeployAsync(plan, origin, ct);

    /// <summary>Arms a library pipeline at a commit (see <see cref="DeploymentService.ArmAsync"/>).</summary>
    public Task ArmAsync(ArmedDeployment armed, string job, CancellationToken ct) => _deployments.ArmAsync(armed, job, ct);

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
