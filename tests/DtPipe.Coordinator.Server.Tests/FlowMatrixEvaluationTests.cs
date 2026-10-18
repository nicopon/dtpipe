using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.FlowControl;
using TransportR.Interfaces;
using TransportR.Services;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// What the flow matrix says of a plan, in the words it says it, and how many times the matrix is asked: when a plan is
/// made, and when it is deployed against the matrix of that day.
/// </summary>
public sealed class FlowMatrixEvaluationTests : IDisposable
{
    private sealed class CountingFlow(IFlowControlService matrix) : IFlowControlService
    {
        private int _evaluations;
        public int Evaluations => Volatile.Read(ref _evaluations);

        public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups)
        {
            Interlocked.Increment(ref _evaluations);
            return matrix.CanSendTo(sourceGroups, targetGroups);
        }
    }

    private sealed class NoHub : IHubContext<NodeChannelHub>
    {
        public IHubClients Clients => throw new NotSupportedException();
        public IGroupManager Groups => throw new NotSupportedException();
    }

    private sealed class NoLink : INodeLink
    {
        public Task<FragmentCheck> CheckAsync(ReplicaKey replica, FragmentSpec spec, CancellationToken ct) => throw new NotSupportedException();
        public Task<LaunchResult> LaunchAsync(ReplicaKey replica, LaunchRequest request, CancellationToken ct) => throw new NotSupportedException();
        public Task<bool> WireAsync(ReplicaKey replica, string runId, string fragment, string alias, string transferId, CancellationToken ct) => throw new NotSupportedException();
        public Task<bool> CancelAsync(ReplicaKey replica, string runId, string fragment, CancellationToken ct) => throw new NotSupportedException();
        public Task<int> KillAsync(ReplicaKey replica, string fragment, CancellationToken ct) => throw new NotSupportedException();
    }

    private sealed class NoRuns : IRunOrchestrator
    {
        public Task<RunResult> RunAsync(RunSpec spec, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<ExitedAck> OnExitedAsync(ReplicaKey replica, FragmentExit exit) => throw new NotSupportedException();
    }

    private const string Fan = "a:\n  input: generate:1\nb:\n  from: a\n  output: csv:b.csv\nc:\n  from: a\n  output: csv:c.csv\n";
    private const string Pair = "a:\n  input: generate:1\nb:\n  from: a\n  output: csv:b.csv\n";

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "matrix-" + Guid.NewGuid().ToString("N"));
    private readonly FlowControlOptions _matrix = new();
    private readonly CountingFlow _flow;
    private readonly ReplicaRegistry _registry = new();
    private readonly NodeInventory _inventory;
    private readonly PlanBuilder _planner;
    private readonly DeploymentManager _deployments;

    public FlowMatrixEvaluationTests()
    {
        Directory.CreateDirectory(_dir);
        var seed = Path.Combine(_dir, "seed.json");
        File.WriteAllText(seed, """{ "identities": [ { "clientId": "node-r1", "name": "r1", "description": "", "groups": ["crm"] }, { "clientId": "node-r2", "name": "r2", "description": "", "groups": ["audit"] }, { "clientId": "node-r3", "name": "r3", "description": "", "groups": ["ops"] } ], "matrix": {} }""");
        var options = new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, "state"), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
        };
        var rights = new RightsStore(options);
        _flow = new CountingFlow(new DefaultFlowControlService(_matrix));
        _inventory = new NodeInventory(_registry);
        _planner = new PlanBuilder(_inventory, new NodeRequests(_inventory, new NoHub()), new BrickCatalog(_inventory), new PlanRegistry(_flow), rights);
        _deployments = TestDeployments.Create(
            _inventory, new NoLink(), new NoRuns(), new ReplicaAdmission(_registry, new InMemoryStateStore()), _flow, rights, new EventBus(),
            MemoryRunStore.Journal(options), options);

        Runner("r1", "crm");
        Runner("r2", "audit");
        Runner("r3", "ops");
    }

    public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

    private void Runner(string name, string group)
    {
        var announcement = new NodeAnnouncement(name, "", [], NodeRole.Runner, [], InstanceId: Guid.NewGuid());
        _registry.Announce("connection-" + name, "node-" + name, announcement, [group], () => { });
        _inventory.Announced(name, "node-" + name, announcement, [group]);
    }

    private void Allow(string from, params string[] to) => _matrix.Groups[from] = new GroupAccess { CanSendTo = [.. to] };

    private Task<DistributionPlan> PlanAsync(string yaml, params (string Unit, string Node)[] placement) =>
        _planner.BuildAsync(new PlanRequest("p", yaml, null, placement.ToDictionary(p => p.Unit, p => p.Node)), PlanMode.Library, TestContext.Current.CancellationToken);

    [Fact]
    public async Task A_Plan_The_Matrix_Allows_Has_No_Rejection_And_Every_Edge_Allowed()
    {
        Allow("crm", "audit");

        var plan = await PlanAsync(Pair, ("a", "r1"), ("b", "r2"));

        Assert.Null(plan.FlowRejection);
        Assert.True(plan.Deployable);
        Assert.True(Assert.Single(plan.Edges).Allowed);
        Assert.Equal(1, _flow.Evaluations);
    }

    [Fact]
    public async Task A_Plan_With_A_Refused_Edge_Says_Which_And_Is_Not_Deployable()
    {
        Allow("crm");

        var plan = await PlanAsync(Pair, ("a", "r1"), ("b", "r2"));

        Assert.Equal("Plan rejected, edge(s) not permitted by the flow control matrix: p@r1.a -> p@r2.a (crm -> audit)", plan.FlowRejection);
        Assert.False(plan.Deployable);
        Assert.False(Assert.Single(plan.Edges).Allowed);
        Assert.Equal(1, _flow.Evaluations);
    }

    /// <summary>The two edges leave the same fragment, so they share their producer's groups: the one allowed and the one refused stay told apart.</summary>
    [Fact]
    public async Task Of_Two_Edges_Leaving_One_Fragment_Only_The_Refused_One_Is_Named_And_Marked()
    {
        Allow("crm", "audit");

        var plan = await PlanAsync(Fan, ("a", "r1"), ("b", "r2"), ("c", "r3"));

        Assert.Equal("Plan rejected, edge(s) not permitted by the flow control matrix: p@r1.a__to__r3 -> p@r3.a (crm -> ops)", plan.FlowRejection);
        Assert.Equal(["a__to__r2", "a__to__r3"], plan.Edges.Select(e => e.ProducerAlias));
        Assert.Equal([true, false], plan.Edges.Select(e => e.Allowed));
        Assert.Equal(2, _flow.Evaluations);
    }

    /// <summary>A plan read back from the library is judged by the matrix of the day it is deployed.</summary>
    [Fact]
    public async Task A_Plan_Deployed_After_The_Matrix_Changed_Is_Refused_In_The_Words_Of_The_Plan()
    {
        Allow("crm", "audit");
        var plan = await PlanAsync(Pair, ("a", "r1"), ("b", "r2"));
        Assert.True(plan.Deployable);
        Allow("crm");
        var before = _flow.Evaluations;

        var refused = await Assert.ThrowsAsync<ConflictException>(() => _deployments.DeployAsync(plan, "test", CancellationToken.None));

        Assert.Equal("Plan rejected, edge(s) not permitted by the flow control matrix: p@r1.a -> p@r2.a (crm -> audit)", refused.Message);
        Assert.Equal(1, _flow.Evaluations - before);
    }
}
