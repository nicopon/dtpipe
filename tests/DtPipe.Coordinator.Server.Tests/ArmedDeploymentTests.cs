using System.Collections.Concurrent;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Models;
using TransportR.Services;
using Xunit;
using DtPipe.Tests.Shared;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A library pipeline is armed at a commit and planned when it is launched: the runner is chosen then, the plan a run ran is
/// journaled with it, and what is armed is kept in a store and read back. Two data nodes (<c>n</c>, <c>m</c>) and two runners
/// (<c>r1</c>, <c>r2</c>) share one state directory across the "restart" the tests make.
/// </summary>
public sealed partial class ArmedDeploymentTests : IDisposable
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);
    private const string Job = "src:\n  input: brick:n/src\nout:\n  from: src\n  output: csv:out.csv\n";
    private const string Commit = "0123456789abcdef";

    private sealed class Replicas : INodeLink
    {
        /// <summary>When set, the replicas answer <c>Check</c> only once it completes: an arming stays under way until then.</summary>
        public Task? Hold { get; set; }

        /// <summary>Completes when a replica is first asked to <c>Check</c>.</summary>
        public TaskCompletionSource Asked { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<FragmentCheck> CheckAsync(ReplicaKey replica, FragmentSpec spec, CancellationToken ct)
        {
            Asked.TrySetResult();
            if (Hold is not null) await Hold;
            return new FragmentCheck(true);
        }
        public Task<int> KillAsync(ReplicaKey replica, string fragment, CancellationToken ct) => Task.FromResult(1);
        public Task<LaunchResult> LaunchAsync(ReplicaKey replica, LaunchRequest request, CancellationToken ct) => throw new NotSupportedException();
        public Task<bool> WireAsync(ReplicaKey replica, string runId, string fragment, string alias, string transferId, CancellationToken ct) => throw new NotSupportedException();
        public Task<bool> CancelAsync(ReplicaKey replica, string runId, string fragment, CancellationToken ct) => throw new NotSupportedException();
    }

    /// <summary>An orchestrator whose runs end as soon as they are sent, remembering what they were sent.</summary>
    private sealed class Runs : IRunOrchestrator
    {
        public ConcurrentQueue<RunSpec> Specs { get; } = new();

        public Task<RunResult> RunAsync(RunSpec spec, CancellationToken ct = default)
        {
            Specs.Enqueue(spec);
            return Task.FromResult(new RunResult(RunOutcome.Succeeded, null, [], new Dictionary<string, FragmentExitReport>(), []));
        }

        public Task<ExitedAck> OnExitedAsync(ReplicaKey replica, FragmentExit exit) => throw new NotSupportedException();
    }

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "armed-" + Guid.NewGuid().ToString("N"));
    private CoordinatorServerOptions _options;
    private readonly FakeJobs _jobs = new();
    private readonly InMemoryStateStore _state = new();
    private readonly Runs _runs = new();
    private readonly Replicas _link = new();
    private RightsStore _rights;
    private ReplicaRegistry _registry = new();
    private NodeInventory _inventory;
    private RunJournal _journal;
    private DeploymentManager _deployments;
    private FailingDeploymentStore _store = null!;
    private int _connections;
    private Func<BrickCatalog, NodeRequests, BindingVerifier>? _verifier;
    private EventBus _bus = new();
    private PipelineHealth _health = null!;
    private readonly FakeHeads _heads = new();
    private readonly Microsoft.Extensions.Time.Testing.FakeTimeProvider _time = new();

    public ArmedDeploymentTests()
    {
        Directory.CreateDirectory(_dir);
        _jobs.Jobs[("p", Commit)] = Job;
        _options = OptionsOf("state", "crm");
        (_rights, _inventory, _journal, _deployments) = Start(_registry);
    }

    private CoordinatorServerOptions OptionsOf(string state, string runnerOneGroup) => new()
    {
        StateDir = Path.Combine(_dir, state), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret",
        RightsSeed = Seed(runnerOneGroup),
    };

    /// <summary>Starts over in a new state directory where runner <c>r1</c> sits in <paramref name="group"/>.</summary>
    private void RunnerOneIn(string group)
    {
        _options = OptionsOf("state-" + group, group);
        Restart();
    }

    public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

    /// <summary>The rights: <c>n</c> and <c>m</c> in <c>crm</c>, runner <c>r2</c> in <c>crm</c>, and <c>r1</c> in <paramref name="runnerOneGroup"/>; crm may send to crm.</summary>
    private string Seed(string runnerOneGroup)
    {
        var path = Path.Combine(_dir, $"seed-{runnerOneGroup}.json");
        File.WriteAllText(path, $$"""
            { "matrix": { "crm": ["crm"] }, "identities": [
              { "clientId": "node-n", "name": "n", "description": "", "groups": ["crm"] },
              { "clientId": "node-m", "name": "m", "description": "", "groups": ["crm"] },
              { "clientId": "node-r1", "name": "r1", "description": "", "groups": ["{{runnerOneGroup}}"] },
              { "clientId": "node-r2", "name": "r2", "description": "", "groups": ["crm"] } ] }
            """);
        return path;
    }

    private (RightsStore, NodeInventory, RunJournal, DeploymentManager) Start(ReplicaRegistry registry)
    {
        var rights = MemoryRightsStore.Rights(_options);
        var inventory = new NodeInventory(registry);
        var journal = MemoryRunStore.Journal(_options);
        _bus = new EventBus();
        BrickCatalog? bricks = null;
        BindingVerifier? verifier = null;
        _store = new FailingDeploymentStore(MemoryDeploymentStore.At(_options.StateDir));
        var deployments = TestDeployments.Create(
            inventory, _link, _runs, new ReplicaAdmission(registry, _state), new RightsFlowControl(rights), rights, _bus, journal, _options, _jobs,
            (b, requests) =>
            {
                bricks = b;
                return verifier = _verifier?.Invoke(b, requests) ?? new BindingVerifier(b, requests);
            },
            _store);
        _health = new PipelineHealth(
            deployments, _jobs, _heads, new AutoPlacer(bricks!, inventory, null!, rights), verifier!, bricks!, inventory, journal, _bus, _options,
            NullLogger<PipelineHealth>.Instance, _time);
        return (rights, inventory, journal, deployments);
    }

    /// <summary>Starts again over the same state directory, with no host connected: what a restart of the coordinator is.</summary>
    private void Restart()
    {
        _registry = new ReplicaRegistry();
        (_rights, _inventory, _journal, _deployments) = Start(_registry);
    }

    private static BrickInfo Brick() => TestBricks.Announced("src", BrickKind.Source, """{"input":"sqlite:a"}""", "source");

    private async Task NodeAsync(string name, string group, NodeRole role, BrickInfo? brick = null)
    {
        var client = Guid.NewGuid();
        var subject = "node-" + name;
        await _state.SaveClientAsync(new ClientRegistration(client, "data-" + name, [group], null, null, DateTime.UtcNow, Subject: subject));
        var announcement = new NodeAnnouncement(
            name, "", [], role, role == NodeRole.Data ? [brick ?? Brick()] : [],
            InstanceId: Guid.NewGuid(), DataPlaneClientId: client);
        var connection = $"c-{name}-{Interlocked.Increment(ref _connections)}";
        _registry.Announce(connection, subject, announcement, [group], () => { });
        _inventory.Announced(name, subject, announcement, [group]);
    }

    private static ArmedDeployment Armed(string node = "n", string job = Job) => new(
        "p", Commit, PipelineLibrary.Sha(job), null, new Dictionary<string, Binding> { ["src"] = new Binding(node, "src", "strict", new BindingPin(Brick().Version)) },
        DateTimeOffset.UtcNow, "ann@x.test");

    private async Task RanAsync(RunView queued)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (!_journal.List().Any(r => r.RunId == queued.RunId))
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException(queued.RunId);
            await Task.Delay(10);
        }
    }

    private RunView Journaled(string runId) => _journal.List().Single(r => r.RunId == runId);

    [Fact]
    public async Task A_Run_Plans_At_Launch_And_Journals_The_Plan_It_Ran()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        var armed = Armed();

        await _deployments.ArmAsync(armed, Job, CancellationToken.None);
        var run = _deployments.Enqueue("p");
        await RanAsync(run);

        Assert.Equal(armed, _deployments.ArmedOf("p"));
        var spec = Assert.Single(_runs.Specs);
        Assert.Equal(["p@n", "p@r1"], spec.Fragments.Select(f => f.Name).Order());
        var done = Journaled(run.RunId);
        Assert.Equal(RunState.Succeeded, done.State);
        var plan = done.Plan!;
        Assert.Equal((done.PlanId, "r1"), (plan.PlanId, plan.Runner));
        Assert.Equal(["p@n", "p@r1"], plan.Fragments.Select(f => f.Name).Order());
        var binding = Assert.Single(plan.Bindings);
        Assert.Equal(("src", "n", "src", "strict", BindingVerdicts.Exact), (binding.Alias, binding.Node, binding.Brick, binding.Mode, binding.Verdict));
        Assert.NotEmpty(binding.BrickVersion);
    }

    [Fact]
    public async Task Arming_A_Reference_No_Check_Pinned_Checks_It_Pins_Its_Version_And_Arms_With_The_Pin()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        var version = Brick().Version;
        var checks = 0;
        var library = new PipelineLibrary(
            new CoordinatorServerOptions
            {
                StateDir = Path.Combine(_dir, "library-state"), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret",
            },
            new AutoPlacer(new BrickCatalog(_inventory), _inventory, null!, _rights).Check);
        await library.SaveAsync("p", Job, "create", CancellationToken.None);
        var validator = new StubValidator(
            _ => new DesignCheck(true, null, new Dictionary<string, CheckedPin> { ["src"] = new("src", new BindingPin(version)) }), _ => checks++);
        var arming = new LibraryArming(library, validator, _deployments, _bus);
        var unpinned = (await library.GetAsync("p", CancellationToken.None))!;
        Assert.Equal(BindingCauses.Unchecked, Assert.Single(unpinned.BindingProblems).Cause);

        var armed = await arming.ArmAsync(unpinned, "ann@x.test", CancellationToken.None);

        var held = (await library.GetAsync("p", CancellationToken.None))!;
        Assert.Empty(held.BindingProblems);
        Assert.Equal(version, held.Bindings["src"].Pin!.Version);
        Assert.Equal(held.Bindings, armed.Bindings);
        Assert.Equal(armed, _deployments.ArmedOf("p"));
        Assert.Equal(1, checks);

        await arming.ArmAsync(held, "ann@x.test", CancellationToken.None);

        Assert.Equal(1, checks);
    }

    [Fact]
    public async Task A_Runner_In_Groups_The_Matrix_Refuses_Is_Skipped_And_The_Next_One_Is_Chosen()
    {
        RunnerOneIn("audit");
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "audit", NodeRole.Runner);
        await NodeAsync("r2", "crm", NodeRole.Runner);

        var launch = await _deployments.PlanLaunchAsync("p", Job, Armed().Bindings, CancellationToken.None);

        Assert.True(launch.Ok);
        Assert.Equal("r2", launch.Runner);
        Assert.Contains(launch.Plan.Fragments, f => f.Node == "r2");
        Assert.DoesNotContain(launch.Plan.Fragments, f => f.Node == "r1");
    }

    [Fact]
    public async Task The_Runners_Are_Tried_By_Name_When_None_Is_Busier_And_A_Launch_No_Runner_Serves_Names_Each()
    {
        RunnerOneIn("audit");
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "audit", NodeRole.Runner);

        var launch = await _deployments.PlanLaunchAsync("p", Job, Armed().Bindings, CancellationToken.None);

        Assert.False(launch.Ok);
        Assert.Null(launch.Runner);
        Assert.Contains("Runner r1:", Assert.Single(launch.Refusals));
        await Assert.ThrowsAsync<ConflictException>(() => _deployments.ArmAsync(Armed(), Job, CancellationToken.None));
        Assert.Null(_deployments.ArmedOf("p"));
    }

    [Fact]
    public async Task A_Job_That_Needs_No_Runner_Launches_Without_One()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("m", "crm", NodeRole.Data);
        const string both = "src:\n  input: brick:n/src\n";
        // A plan with every branch bound is refused here only for what it lacks, never for a runner it does not use.
        var launch = await _deployments.PlanLaunchAsync("p", both, new Dictionary<string, Binding> { ["src"] = new Binding("n", "src", "strict", new BindingPin(Brick().Version)) }, CancellationToken.None);

        Assert.Null(launch.Runner);
        Assert.DoesNotContain(launch.Plan.Errors, e => e.Contains("runner"));
    }

    [Fact]
    public async Task A_Restart_Reads_The_Armed_Deployment_Back_As_Ready_And_A_Run_Is_Refused_Naming_What_Is_Not_Back()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        var armed = Armed();
        await _deployments.ArmAsync(armed, Job, CancellationToken.None);
        Assert.Equal(["p"], StoredPipelines());

        Restart();

        Assert.Equal(armed.Commit, _deployments.ArmedOf("p")!.Commit);
        Assert.Equal(armed.Bindings, _deployments.ArmedOf("p")!.Bindings);
        var view = Assert.Single(await _deployments.DeploymentsAsync());
        Assert.Equal((DeploymentState.Ready, Commit, "library"), (view.State, view.Commit, view.Origin));
        Assert.Equal(["n"], view.Nodes);
        // No replica is connected yet: the run is refused, naming the node of the binding, and is not an error.
        var run = _deployments.Enqueue("p");
        await RanAsync(run);
        var refused = Journaled(run.RunId);
        Assert.Equal(RunState.Refused, refused.State);
        // The node has an identity and has not announced: it is absent, not a text that stopped being its brick.
        Assert.Contains("'src' references brick 'src' on n, which has no connected replica", refused.Refusal);
        Assert.DoesNotContain("no longer that brick", refused.Refusal);
        Assert.Empty(_runs.Specs);

        // The nodes come back: the same deployment runs.
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        var again = _deployments.Enqueue("p");
        await RanAsync(again);
        Assert.Equal(RunState.Succeeded, Journaled(again.RunId).State);
    }

    [Fact]
    public async Task A_Deployment_Whose_Node_Lost_Its_Identity_Stays_Armed_And_Its_Run_Is_Refused_By_Name()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        await _deployments.ArmAsync(Armed(), Job, CancellationToken.None);
        _rights.RemoveIdentity("node-n");
        Restart();

        var run = _deployments.Enqueue("p");
        await RanAsync(run);

        Assert.NotNull(_deployments.ArmedOf("p"));
        var refused = Journaled(run.RunId);
        Assert.Equal(RunState.Refused, refused.State);
        Assert.Contains("'src' references brick 'src' on n", refused.Refusal);
    }

    [Fact]
    public async Task A_Run_Reads_The_Job_At_The_Armed_Commit_And_Is_Refused_When_The_Library_Cannot_Give_It()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        await _deployments.ArmAsync(Armed(), Job, CancellationToken.None);
        Restart();
        _jobs.Jobs.Clear();
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);

        var run = _deployments.Enqueue("p");
        await RanAsync(run);

        var refused = Journaled(run.RunId);
        Assert.Equal(RunState.Refused, refused.State);
        Assert.Contains("cannot be read at 01234567", refused.Refusal);
    }

    private IReadOnlyList<string> StoredPipelines() => MemoryDeploymentStore.At(_options.StateDir).Pipelines();

    [Fact]
    public async Task Undeploying_Or_Replacing_An_Armed_Deployment_Leaves_No_Record()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        await _deployments.ArmAsync(Armed(), Job, CancellationToken.None);
        Assert.Equal(["p"], StoredPipelines());

        await _deployments.UndeployAsync("p", CancellationToken.None);
        Assert.Empty(StoredPipelines());

        await _deployments.ArmAsync(Armed(), Job, CancellationToken.None);
        var fixedPlan = (await _deployments.PlanLaunchAsync("p", Job, Armed().Bindings, CancellationToken.None)).Plan;
        await _deployments.DeployAsync(fixedPlan, "lab", CancellationToken.None);
        Assert.Empty(StoredPipelines());
        Assert.Null(_deployments.ArmedOf("p"));
    }

    [Fact]
    public async Task A_Store_That_Is_Down_Leaves_The_Armed_Deployment_Where_It_Was()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        var armed = Armed();
        await _deployments.ArmAsync(armed, Job, CancellationToken.None);

        _store.Down = true;
        await Assert.ThrowsAsync<StoreUnavailableException>(() => _deployments.UndeployAsync("p", CancellationToken.None));
        await Assert.ThrowsAsync<StoreUnavailableException>(() => _deployments.ArmAsync(Armed(), Job, CancellationToken.None));

        // Nothing changed: the deployment is armed, ready and kept, and no refusal was published for an arming that never began.
        Assert.Equal(armed.Commit, _deployments.ArmedOf("p")!.Commit);
        Assert.Equal(DeploymentState.Ready, Assert.Single(await _deployments.DeploymentsAsync()).State);
        _store.Down = false;
        Assert.Equal(["p"], StoredPipelines());
    }

    [Fact]
    public async Task An_Arming_The_Store_Cannot_Keep_Is_Refused_And_Not_Left_In_Memory()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        var hold = new TaskCompletionSource();
        _link.Hold = hold.Task;

        // The store is up when the arming begins and down when it would keep the deployment.
        var arming = _deployments.ArmAsync(Armed(), Job, CancellationToken.None);
        await _link.Asked.Task.WaitAsync(Wait);
        _store.Down = true;
        hold.SetResult();

        await Assert.ThrowsAsync<StoreUnavailableException>(() => arming);
        Assert.Null(_deployments.ArmedOf("p"));
        Assert.Empty(await _deployments.DeploymentsAsync());
        _store.Down = false;
        Assert.Empty(StoredPipelines());
    }

    // ---- Bindings in a mode other than strict ----

    /// <summary>The brick as its owner changed it since the binding was checked: the job still holds the old text.</summary>
    private static BrickInfo Changed(string input = "sqlite:b") => TestBricks.Announced("src", BrickKind.Source, $$"""{"input":"{{input}}"}""", "source");

    private static string VersionOf(BrickInfo brick) => brick.Version;

    private static ArmedDeployment ArmedWith(Binding binding) => new(
        "p", Commit, PipelineLibrary.Sha(Job), null, new Dictionary<string, Binding> { ["src"] = binding }, DateTimeOffset.UtcNow, "ann@x.test");

    /// <summary>A verifier that answers a node's question itself and counts how often it was asked.</summary>
    private sealed class Asked(BrickCatalog bricks, NodeRequests nodes, Func<BindingVerdict> answer) : BindingVerifier(bricks, nodes)
    {
        public int Count;

        protected override Task<BindingVerdict> AskAsync(BrickView brick, BindingPin pin, string mode)
        {
            Interlocked.Increment(ref Count);
            return Task.FromResult(answer());
        }
    }

    private Asked UseVerifier(Func<BindingVerdict> answer)
    {
        Asked? made = null;
        _verifier = (bricks, nodes) => made = new Asked(bricks, nodes, answer);
        Restart();
        return made!;
    }

    [Fact]
    public async Task A_Tolerant_Binding_Whose_Brick_Changed_Runs_And_Asks_No_Node()
    {
        // The default verifier reaches a node through a hub that does not exist: any question would refuse the run.
        await NodeAsync("n", "crm", NodeRole.Data, Changed());
        await NodeAsync("r1", "crm", NodeRole.Runner);
        await _deployments.ArmAsync(ArmedWith(new Binding("n", "src", "tolerant", new BindingPin("an-older-version"))), Job, CancellationToken.None);

        var run = _deployments.Enqueue("p");
        await RanAsync(run);

        var done = Journaled(run.RunId);
        Assert.Equal(RunState.Succeeded, done.State);
        var binding = Assert.Single(done.Plan!.Bindings);
        Assert.Equal(("tolerant", BindingVerdicts.Tolerated, VersionOf(Changed())), (binding.Mode, binding.Verdict, binding.BrickVersion));
        // The fragment is the brick as it is now, not the text the job holds.
        var fragment = Assert.Single(_runs.Specs.Single().Fragments, f => f.Name == "p@n");
        Assert.Equal("src", Assert.Single(fragment.Spec.Units!, u => u.Key == "src").Brick);
    }

    [Fact]
    public async Task A_Brick_At_The_Version_Its_Binding_Was_Checked_At_Is_Unchanged()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        await _deployments.ArmAsync(ArmedWith(new Binding("n", "src", "contract", new BindingPin(VersionOf(Brick()), "{}"))), Job, CancellationToken.None);

        var run = _deployments.Enqueue("p");
        await RanAsync(run);

        Assert.Equal(BindingVerdicts.Unchanged, Assert.Single(Journaled(run.RunId).Plan!.Bindings).Verdict);
    }

    [Fact]
    public async Task A_Binding_In_Another_Mode_With_No_Pin_Blocks_The_Plan_By_Name()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);

        var none = await _deployments.PlanLaunchAsync("p", Job, new Dictionary<string, Binding> { ["src"] = new Binding("n", "src", "tolerant") }, CancellationToken.None);
        var noContract = await _deployments.PlanLaunchAsync("p", Job, new Dictionary<string, Binding> { ["src"] = new Binding("n", "src", "compatible", new BindingPin("v")) }, CancellationToken.None);

        Assert.False(none.Ok);
        Assert.Contains("'src'", none.Refusal);
        Assert.Contains("no check has pinned it", none.Refusal);
        Assert.False(noContract.Ok);
        await Assert.ThrowsAsync<ConflictException>(() => _deployments.ArmAsync(ArmedWith(new Binding("n", "src", "contract")), Job, CancellationToken.None));
    }

    [Fact]
    public async Task A_Breaking_Verdict_Refuses_The_Run_Naming_The_Binding_And_Is_Kept_For_The_Version()
    {
        var node = UseVerifier(() => new BindingVerdict(BindingVerdicts.Breaking, "id: int64 → int32"));
        await NodeAsync("n", "crm", NodeRole.Data, Changed());
        await NodeAsync("r1", "crm", NodeRole.Runner);
        await _deployments.ArmAsync(ArmedWith(new Binding("n", "src", "contract", new BindingPin("an-older-version", "{}"))), Job, CancellationToken.None);

        var first = _deployments.Enqueue("p");
        await RanAsync(first);
        var second = _deployments.Enqueue("p");
        await RanAsync(second);

        var refused = Journaled(first.RunId);
        Assert.Equal(RunState.Refused, refused.State);
        Assert.Contains("'src' is bound to brick 'src' on n in mode 'contract'", refused.Refusal);
        Assert.Contains("id: int64 → int32", refused.Refusal);
        Assert.Empty(_runs.Specs);
        // One brick version, one verdict: the second run does not ask again.
        Assert.Equal(RunState.Refused, Journaled(second.RunId).State);
        Assert.Equal(1, node.Count);
    }

    [Fact]
    public async Task A_Compatible_Verdict_Lets_The_Run_Through_And_Is_Journaled()
    {
        var node = UseVerifier(() => new BindingVerdict(BindingVerdicts.Compatible, "id: int32 → int64, wider"));
        await NodeAsync("n", "crm", NodeRole.Data, Changed());
        await NodeAsync("r1", "crm", NodeRole.Runner);
        await _deployments.ArmAsync(ArmedWith(new Binding("n", "src", "compatible", new BindingPin("an-older-version", "{}"))), Job, CancellationToken.None);

        var run = _deployments.Enqueue("p");
        await RanAsync(run);
        var again = _deployments.Enqueue("p");
        await RanAsync(again);

        Assert.Equal(BindingVerdicts.Compatible, Assert.Single(Journaled(run.RunId).Plan!.Bindings).Verdict);
        Assert.Equal(RunState.Succeeded, Journaled(again.RunId).State);
        Assert.Equal(1, node.Count);
    }

    [Fact]
    public async Task A_Node_That_Cannot_Tell_Refuses_The_Run_And_The_Question_Is_Asked_Again()
    {
        var node = UseVerifier(() => new BindingVerdict(BindingVerdicts.Unread, "n is offline."));
        await NodeAsync("n", "crm", NodeRole.Data, Changed());
        await NodeAsync("r1", "crm", NodeRole.Runner);
        await _deployments.ArmAsync(ArmedWith(new Binding("n", "src", "compatible", new BindingPin("an-older-version", "{}"))), Job, CancellationToken.None);

        var first = _deployments.Enqueue("p");
        await RanAsync(first);
        var second = _deployments.Enqueue("p");
        await RanAsync(second);

        Assert.Contains("could not be checked: n is offline.", Journaled(first.RunId).Refusal);
        Assert.Equal(RunState.Refused, Journaled(second.RunId).State);
        Assert.Equal(2, node.Count);
    }

    [Fact]
    public async Task A_Binding_Names_The_Node_That_Hosts_Its_Brick_Even_When_An_Earlier_Node_Offers_The_Same_One()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("m", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);

        var launch = await _deployments.PlanLaunchAsync("p", Job, new Dictionary<string, Binding> { ["src"] = new Binding("n", "src", "strict", new BindingPin(Brick().Version)) }, CancellationToken.None);

        Assert.True(launch.Ok, launch.Refusal);
        Assert.Contains(launch.Plan.Fragments, f => f.Node == "n");
        Assert.DoesNotContain(launch.Plan.Fragments, f => f.Node == "m");
    }

    [Fact]
    public async Task An_Armed_Pin_Is_Read_Back_With_Its_Contract_After_A_Restart()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        var armed = ArmedWith(new Binding("n", "src", "compatible", new BindingPin("a-version", "{\"hash\":\"abcdef0123456789\"}")));
        await _deployments.ArmAsync(armed, Job, CancellationToken.None);

        Restart();

        Assert.Equal(armed.Bindings, _deployments.ArmedOf("p")!.Bindings);
    }

    [Fact]
    public async Task A_Verdict_Is_Absent_Until_Asked_Then_Known_Without_Asking_A_Node_Again()
    {
        var node = UseVerifier(() => new BindingVerdict(BindingVerdicts.Compatible));
        await NodeAsync("n", "crm", NodeRole.Data, Changed());
        await NodeAsync("r1", "crm", NodeRole.Runner);
        var binding = new Binding("n", "src", "compatible", new BindingPin("an-older-version", "{}"));
        await _deployments.ArmAsync(ArmedWith(binding), Job, CancellationToken.None);
        Assert.Equal(VerdictState.Absent, node.StateOf("p", Commit, "src", binding));

        var run = _deployments.Enqueue("p");
        await RanAsync(run);

        Assert.Equal(VerdictState.Known, node.StateOf("p", Commit, "src", binding));
        Assert.Equal(1, node.Count);
        // A strict binding, a brick at its pin and a tolerant one need no node: known at once.
        Assert.Equal(VerdictState.Known, node.StateOf("p", Commit, "src", new Binding("n", "src", "strict")));
        Assert.Equal(VerdictState.Known, node.StateOf("p", Commit, "src", binding with { Mode = "tolerant" }));
    }

    [Fact]
    public async Task After_A_Restart_Before_Any_Node_Announces_Every_Mode_Is_Refused_For_The_Node_Being_Absent()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        var strict = new Dictionary<string, Binding> { ["src"] = new Binding("n", "src", "strict") };
        var tolerant = new Dictionary<string, Binding> { ["src"] = new Binding("n", "src", "tolerant", new BindingPin("v")) };
        Restart();
        var placer = new AutoPlacer(new BrickCatalog(_inventory), _inventory, null!, _rights);

        var strictLaunch = await _deployments.PlanLaunchAsync("p", Job, strict, CancellationToken.None);
        var tolerantLaunch = await _deployments.PlanLaunchAsync("p", Job, tolerant, CancellationToken.None);

        // The rights know the node and the inventory does not yet: the cause is its absence, in every mode, and not a text that stopped
        // being a brick (strict) or a node nobody knows (another mode).
        foreach (var (bindings, launch) in new[] { (strict, strictLaunch), (tolerant, tolerantLaunch) })
        {
            var problem = Assert.Single(placer.Check(Job, bindings));
            Assert.Equal((BindingCauses.Offline, false), (problem.Cause, problem.Blocking));
            Assert.False(launch.Ok);
            Assert.Contains("'src' references brick 'src' on n, which has no connected replica", launch.Refusal);
        }
    }
}
