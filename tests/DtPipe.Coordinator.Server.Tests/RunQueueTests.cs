using System.Collections.Concurrent;
using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using TransportR.Models;
using TransportR.Services;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// What the deployments and the run queue do between them, with a scripted orchestrator whose runs the test ends and a
/// scripted node link: the order runs start in, what cancels them, what a deployment in progress holds back, the words a
/// state is written with, and the events the pages read. Three nodes: <c>n</c> and <c>m</c> carry data, <c>r</c> is a runner.
/// </summary>
public sealed class RunQueueTests : IDisposable
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private sealed class Replicas : INodeLink
    {
        public TaskCompletionSource<FragmentCheck>? CheckGate { get; set; }
        public FragmentCheck Answer { get; set; } = new(true);
        public ConcurrentQueue<(ReplicaKey Replica, string Fragment)> Killed { get; } = new();

        public Task<FragmentCheck> CheckAsync(ReplicaKey replica, FragmentSpec spec, CancellationToken ct) =>
            CheckGate is { } gate ? gate.Task : Task.FromResult(Answer);

        public Task<int> KillAsync(ReplicaKey replica, string fragment, CancellationToken ct)
        {
            Killed.Enqueue((replica, fragment));
            return Task.FromResult(1);
        }

        public Task<LaunchResult> LaunchAsync(ReplicaKey replica, LaunchRequest request, CancellationToken ct) => throw new NotSupportedException();
        public Task<bool> WireAsync(ReplicaKey replica, string runId, string fragment, string alias, string transferId, CancellationToken ct) => throw new NotSupportedException();
        public Task<bool> CancelAsync(ReplicaKey replica, string runId, string fragment, CancellationToken ct) => throw new NotSupportedException();
    }

    /// <summary>An orchestrator whose runs end when the test says so, or when their token is cancelled, as the real one's do.</summary>
    private sealed class ScriptedRuns : IRunOrchestrator
    {
        public ConcurrentQueue<RunSpec> Specs { get; } = new();
        public ConcurrentDictionary<string, TaskCompletionSource<RunResult>> Ends { get; } = new();
        public Func<RunSpec, Exception?> Fails { get; set; } = _ => null;

        public Task<RunResult> RunAsync(RunSpec spec, CancellationToken ct = default)
        {
            Specs.Enqueue(spec);
            if (Fails(spec) is { } failure) return Task.FromException<RunResult>(failure);
            var end = Ends.GetOrAdd(spec.RunId, _ => new(TaskCreationOptions.RunContinuationsAsynchronously));
            ct.Register(() => end.TrySetResult(Result(RunOutcome.Cancelled)));
            return end.Task;
        }

        public Task<ExitedAck> OnExitedAsync(ReplicaKey replica, FragmentExit exit) => throw new NotSupportedException();

        public void End(string runId, RunOutcome outcome) =>
            Ends.GetOrAdd(runId, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult(Result(outcome));

        public bool Saw(string runId) => Specs.Any(s => s.RunId == runId);
    }

    private static RunResult Result(RunOutcome outcome) =>
        new(outcome, null, [], new Dictionary<string, FragmentExitReport>(), []);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "queue-" + Guid.NewGuid().ToString("N"));
    private readonly RightsStore _rights;
    private readonly ReplicaRegistry _registry = new();
    private readonly InMemoryStateStore _state = new();
    private readonly NodeInventory _inventory;
    private readonly Replicas _link = new();
    private readonly ScriptedRuns _runs = new();
    private readonly FakeJobs _jobs = new();
    private readonly CoordinatorServerOptions _options;
    private readonly FailingRunStore _store;
    private readonly RunJournal _journal;
    private readonly EventBus _bus = new();
    private readonly DeploymentManager _deployments;
    private readonly ConcurrentDictionary<string, List<string>> _connectionsOf = new();
    private int _connections;

    public RunQueueTests()
    {
        Directory.CreateDirectory(_dir);
        var seed = Path.Combine(_dir, "seed.json");
        File.WriteAllText(seed, """
            { "matrix": {}, "identities": [
              { "clientId": "node-n", "name": "n", "description": "", "groups": ["crm"] },
              { "clientId": "node-m", "name": "m", "description": "", "groups": ["crm"] },
              { "clientId": "node-r", "name": "r", "description": "", "groups": ["crm"] } ] }
            """);
        var options = _options = new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, "state"), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
        };
        _rights = new RightsStore(options);
        _inventory = new NodeInventory(_registry);
        _store = new FailingRunStore(MemoryRunStore.At(options.StateDir));
        _journal = new RunJournal(_store, NullLogger<RunJournal>.Instance);
        _deployments = TestDeployments.Create(
            _inventory, _link, _runs, new ReplicaAdmission(_registry, _state), new RightsFlowControl(_rights), _rights, _bus,
            _journal, options, _jobs);
    }

    public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

    private static readonly FragmentUnit[] Units = [new("src", FragmentUnitKind.SourceSend, "src")];

    private static BrickInfo Brick() => new("src", BrickKind.Source, "source", "", """{"input":"sqlite:a"}""", null, null);

    /// <summary>One replica of the node <paramref name="name"/>, its TransportR client registered under the node's subject.</summary>
    private async Task<ReplicaKey> ReplicaAsync(string name, NodeRole role = NodeRole.Data)
    {
        var client = Guid.NewGuid();
        var subject = "node-" + name;
        await _state.SaveClientAsync(new ClientRegistration(client, "data-" + name, ["crm"], null, null, DateTime.UtcNow, Subject: subject));
        var announcement = new NodeAnnouncement(
            name, "", [], role, role == NodeRole.Data ? [Brick()] : [], Capabilities: [NodeAnnouncement.LaunchToken],
            InstanceId: Guid.NewGuid(), DataPlaneClientId: client);
        var connection = $"c-{name}-{Interlocked.Increment(ref _connections)}";
        _connectionsOf.GetOrAdd(name, _ => []).Add(connection);
        var key = _registry.Announce(connection, subject, announcement, ["crm"], () => { });
        _inventory.Announced(name, subject, announcement, ["crm"]);
        return key;
    }

    private static DistributionPlan DataPlan(string pipeline, string node)
    {
        var composed = FragmentJob.Compose(Units, FragmentJob.Over([Brick()])).Yaml!;
        var fragment = new PlannedFragment(
            $"{pipeline}@{node}", node, ["crm"], "", FragmentJob.VersionOf(composed), [], ["src"], FragmentForm.Bricks, Composition: Units);
        return new DistributionPlan("plan-" + pipeline, pipeline, [], [], [fragment], [], [], [], null);
    }

    private static DistributionPlan RunnerPlan(string pipeline)
    {
        const string yaml = "x:\n  input: generate:1\n";
        var fragment = new PlannedFragment($"{pipeline}@r", "r", ["crm"], yaml, FragmentJob.VersionOf(yaml), [], ["x"], FragmentForm.Job);
        return new DistributionPlan("plan-" + pipeline, pipeline, [], [], [fragment], [], [], [], null);
    }

    private Task DeployAsync(DistributionPlan plan) => _deployments.DeployAsync(plan, "test", CancellationToken.None);

    private static async Task UntilAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException(what);
            await Task.Delay(10);
        }
    }

    private Task RunningAsync(string runId) =>
        UntilAsync(() => _deployments.Queue.Any(r => r.RunId == runId && r.State == RunState.Running), $"{runId} to run");

    private Task FinishedAsync(string runId) =>
        UntilAsync(() => _journal.List().Any(r => r.RunId == runId), $"{runId} to be journaled");

    private RunView Journaled(string runId) => _journal.List().Single(r => r.RunId == runId);

    [Fact]
    public async Task A_Run_Is_Refused_While_Nothing_Is_Deployed_Or_Its_Pipeline_Is_Not()
    {
        var none = Assert.Throws<ConflictException>(() => _deployments.Enqueue(null));
        Assert.Equal("Deploy a plan first.", none.Message);

        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));
        var absent = Assert.Throws<ConflictException>(() => _deployments.Enqueue("q"));
        Assert.Equal("'q' is not deployed.", absent.Message);
    }

    [Fact]
    public async Task A_Run_Goes_Through_The_Orchestrator_And_Is_Journaled_With_The_Word_Of_Its_Outcome()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));

        var queued = _deployments.Enqueue(null);
        Assert.Equal(RunState.Queued, queued.State);
        await RunningAsync(queued.RunId);

        var spec = Assert.Single(_runs.Specs);
        Assert.Equal(queued.RunId, spec.RunId);
        var fragment = Assert.Single(spec.Fragments);
        Assert.Equal(("p@n", "node-n"), (fragment.Name, fragment.Node));
        Assert.Equal(["crm"], fragment.Groups);
        Assert.Equal(queued.RunId, _deployments.CurrentRun("p")!.RunId);
        var view = Assert.Single(await _deployments.DeploymentsAsync());
        Assert.Equal((0, queued.RunId), (view.Queued, view.RunningRunId));

        _runs.End(queued.RunId, RunOutcome.Succeeded);
        await FinishedAsync(queued.RunId);

        var done = Journaled(queued.RunId);
        Assert.Equal((RunState.Succeeded, "Outcome: Succeeded"), (done.State, done.Description));
        Assert.NotNull(done.FinishedAt);
        Assert.Empty(_deployments.Queue);
        Assert.Equal(queued.RunId, _deployments.CurrentRun("p")!.RunId);
    }

    [Fact]
    public async Task A_Run_Names_Who_Launched_It_In_Flight_And_In_The_Journal()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));

        var run = _deployments.Enqueue("p", "ann@x.test");
        await RunningAsync(run.RunId);
        Assert.Equal("ann@x.test", _deployments.Queue.Single().LaunchedBy);
        _runs.End(run.RunId, RunOutcome.Succeeded);
        await FinishedAsync(run.RunId);

        var journaled = Journaled(run.RunId);
        Assert.Equal(("ann@x.test", null), (journaled.LaunchedBy, journaled.IdempotencyKey));
    }

    [Fact]
    public async Task A_Second_Request_With_The_Same_Key_Answers_The_First_Run_Whether_It_Waits_Runs_Or_Is_Over()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));

        var first = _deployments.Launch("p", "cron", "night-1");
        await RunningAsync(first.Run.RunId);
        var running = _deployments.Launch("p", "cron", "night-1");
        _runs.End(first.Run.RunId, RunOutcome.Succeeded);
        await FinishedAsync(first.Run.RunId);
        var over = _deployments.Launch("p", "cron", "night-1");

        Assert.False(first.Replayed);
        Assert.Equal((first.Run.RunId, true, first.Run.RunId, true), (running.Run.RunId, running.Replayed, over.Run.RunId, over.Replayed));
        Assert.Equal(RunState.Succeeded, over.Run.State);
        Assert.Equal(("cron", "night-1"), (over.Run.LaunchedBy, over.Run.IdempotencyKey));
        Assert.Single(_runs.Specs);
        Assert.Single(_journal.List());
    }

    [Fact]
    public async Task Requests_Racing_With_One_Key_Make_One_Run()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));

        var answers = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(() => _deployments.Launch("p", "cron", "night-1"))));

        Assert.Single(answers.Select(a => a.Run.RunId).Distinct());
        Assert.Single(answers, a => !a.Replayed);
    }

    [Fact]
    public async Task A_Key_Belongs_To_The_Caller_That_Sent_It_And_A_Request_Without_One_Is_Never_A_Replay()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));

        var cron = _deployments.Launch("p", "cron", "k");
        var other = _deployments.Launch("p", "other", "k");
        var keyless = _deployments.Launch("p", "cron", null);
        var again = _deployments.Launch("p", "cron", null);

        Assert.Equal(4, new[] { cron, other, keyless, again }.Select(a => a.Run.RunId).Distinct().Count());
        Assert.All(new[] { cron, other, keyless, again }, a => Assert.False(a.Replayed));
    }

    [Fact]
    public async Task The_Same_Key_For_Another_Pipeline_Is_A_Conflict_And_Starts_Nothing()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));
        _deployments.Launch("p", "cron", "night-1");

        var refused = Assert.Throws<ConflictException>(() => _deployments.Launch("q", "cron", "night-1"));

        Assert.Contains("'p'", refused.Message);
        Assert.Single(_deployments.Queue);
    }

    [Fact]
    public async Task A_Key_Is_Answered_After_A_Restart_As_Long_As_The_Run_Is_Journaled()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));
        var first = _deployments.Launch("p", "cron", "night-1");
        await RunningAsync(first.Run.RunId);
        _runs.End(first.Run.RunId, RunOutcome.Failed);
        await FinishedAsync(first.Run.RunId);

        var restarted = TestDeployments.Create(
            _inventory, _link, new ScriptedRuns(), new ReplicaAdmission(_registry, _state), new RightsFlowControl(_rights), _rights, new EventBus(),
            MemoryRunStore.Journal(_options), _options, _jobs);
        var replay = restarted.Launch("p", "cron", "night-1");

        Assert.Equal((first.Run.RunId, true, RunState.Failed), (replay.Run.RunId, replay.Replayed, replay.Run.State));
        Assert.Empty(restarted.Queue);
    }

    [Fact]
    public async Task A_Restart_Ends_The_Runs_It_Interrupted_Keeps_Their_Keys_And_Never_Reuses_Their_Ids()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));
        var running = _deployments.Launch("p", "cron", "night-1");
        await RunningAsync(running.Run.RunId);
        var waiting = _deployments.Launch("p", "cron", "night-2");
        Assert.Equal(RunState.Queued, waiting.Run.State);

        var journal = MemoryRunStore.Journal(_options);
        var restarted = TestDeployments.Create(
            _inventory, _link, new ScriptedRuns(), new ReplicaAdmission(_registry, _state), new RightsFlowControl(_rights), _rights, new EventBus(),
            journal, _options, _jobs);

        Assert.Equal([RunState.Error, RunState.Error], new[] { running.Run.RunId, waiting.Run.RunId }.Select(id => journal.Find(r => r.RunId == id)!.State));
        Assert.Equal(running.Run.RunId, Assert.Single(journal.Owed).RunId);
        Assert.Equal(running.Run.RunId, restarted.Launch("p", "cron", "night-1").Run.RunId);
        Assert.Empty(restarted.Queue);
        Assert.True(int.Parse(journal.NextRunId().Split('-').Last()) > int.Parse(waiting.Run.RunId.Split('-').Last()));
    }

    [Fact]
    public async Task A_Launch_The_Store_Cannot_Journal_Is_Refused_And_Leaves_Nothing_In_The_Queue()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));
        _store.Down = true;

        Assert.Throws<StoreUnavailableException>(() => _deployments.Launch("p", "cron", "night-1"));
        Assert.Empty(_deployments.Queue);

        _store.Down = false;
        var launched = _deployments.Launch("p", "cron", "night-1");
        Assert.False(launched.Replayed);
    }

    [Fact]
    public async Task A_Cancelled_Waiting_Run_Still_Answers_Its_Key()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));
        var running = _deployments.Enqueue("p");
        var waiting = _deployments.Launch("p", "cron", "night-1");
        await RunningAsync(running.RunId);

        _deployments.Cancel(waiting.Run.RunId);
        var replay = _deployments.Launch("p", "cron", "night-1");

        Assert.Equal((waiting.Run.RunId, true, RunState.Cancelled), (replay.Run.RunId, replay.Replayed, replay.Run.State));
        Assert.Single(_deployments.Queue);
    }

    [Fact]
    public async Task A_Run_Is_Found_Waiting_Running_And_Over()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));
        var running = _deployments.Enqueue("p");
        var waiting = _deployments.Enqueue("p");
        await RunningAsync(running.RunId);

        Assert.Equal((RunState.Running, RunState.Queued), (_deployments.FindRun(running.RunId)!.State, _deployments.FindRun(waiting.RunId)!.State));
        _runs.End(running.RunId, RunOutcome.Succeeded);
        await FinishedAsync(running.RunId);
        Assert.Equal(RunState.Succeeded, _deployments.FindRun(running.RunId)!.State);
        Assert.Null(_deployments.FindRun("run-999"));
    }

    [Theory]
    [InlineData(RunOutcome.Failed, RunState.Failed)]
    [InlineData(RunOutcome.Cancelled, RunState.Cancelled)]
    public async Task The_State_Of_A_Finished_Run_Is_The_One_Of_Its_Outcome(RunOutcome outcome, RunState state)
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));
        var run = _deployments.Enqueue("p");
        await RunningAsync(run.RunId);

        _runs.End(run.RunId, outcome);
        await FinishedAsync(run.RunId);

        Assert.Equal(state, Journaled(run.RunId).State);
    }

    [Fact]
    public async Task A_Refusal_Of_Admission_Is_Refused_And_Any_Other_Failure_Is_An_Error()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));
        _runs.Fails = spec => spec.RunId.EndsWith('1')
            ? new NoEligibleReplicaException(new Dictionary<string, string> { ["p@n"] = "none" })
            : new InvalidOperationException("boom");

        var first = _deployments.Enqueue("p");
        await FinishedAsync(first.RunId);
        var second = _deployments.Enqueue("p");
        await FinishedAsync(second.RunId);

        var refused = Journaled(first.RunId);
        Assert.Equal(RunState.Refused, refused.State);
        Assert.Contains("none", refused.Refusal);
        var error = Journaled(second.RunId);
        Assert.Equal((RunState.Error, "boom"), (error.State, error.Refusal));
    }

    [Fact]
    public async Task One_Run_At_A_Time_Per_Data_Node_In_The_Order_They_Came_And_A_Runner_Never_Counts()
    {
        await ReplicaAsync("n");
        await ReplicaAsync("m");
        await ReplicaAsync("r", NodeRole.Runner);
        await DeployAsync(DataPlan("p", "n"));
        await DeployAsync(DataPlan("q", "n"));
        await DeployAsync(DataPlan("s", "m"));
        await DeployAsync(RunnerPlan("t"));
        await DeployAsync(RunnerPlan("u"));

        var p = _deployments.Enqueue("p");
        var q = _deployments.Enqueue("q");
        var s = _deployments.Enqueue("s");
        var t = _deployments.Enqueue("t");
        var u = _deployments.Enqueue("u");
        await Task.WhenAll(RunningAsync(p.RunId), RunningAsync(s.RunId), RunningAsync(t.RunId), RunningAsync(u.RunId));

        Assert.False(_runs.Saw(q.RunId));
        Assert.Equal(RunState.Queued, _deployments.Queue.Single(r => r.RunId == q.RunId).State);
        Assert.Equal(q.RunId, _deployments.Queue.Last().RunId);
        var views = await _deployments.DeploymentsAsync();
        var waiting = views.Single(d => d.PipelineId == "q");
        Assert.Equal((1, (string?)null), (waiting.Queued, waiting.RunningRunId));
        var running = views.Single(d => d.PipelineId == "p");
        Assert.Equal((0, p.RunId), (running.Queued, running.RunningRunId));

        _runs.End(p.RunId, RunOutcome.Succeeded);
        await RunningAsync(q.RunId);
        foreach (var run in new[] { q, s, t, u }) _runs.End(run.RunId, RunOutcome.Succeeded);
        await Task.WhenAll(new[] { p, q, s, t, u }.Select(r => FinishedAsync(r.RunId)));
    }

    [Fact]
    public async Task Cancel_With_No_Id_Cancels_The_Only_Run_In_Flight_And_Refuses_To_Guess_Among_Several()
    {
        await ReplicaAsync("n");
        await ReplicaAsync("m");
        await DeployAsync(DataPlan("p", "n"));
        await DeployAsync(DataPlan("s", "m"));

        _deployments.Cancel();
        Assert.Empty(_deployments.Queue);

        var p = _deployments.Enqueue("p");
        await RunningAsync(p.RunId);
        _deployments.Cancel();
        await FinishedAsync(p.RunId);
        Assert.Equal(RunState.Cancelled, Journaled(p.RunId).State);

        var again = _deployments.Enqueue("p");
        var s = _deployments.Enqueue("s");
        await Task.WhenAll(RunningAsync(again.RunId), RunningAsync(s.RunId));
        var ex = Assert.Throws<ConflictException>(() => _deployments.Cancel());
        Assert.Equal("2 runs are in flight: name the one to cancel.", ex.Message);
        Assert.Equal(2, _deployments.Queue.Count(r => r.State == RunState.Running));

        _deployments.Cancel(s.RunId);
        await FinishedAsync(s.RunId);
        Assert.Equal(RunState.Cancelled, Journaled(s.RunId).State);
        _runs.End(again.RunId, RunOutcome.Succeeded);
        await FinishedAsync(again.RunId);
    }

    [Fact]
    public async Task A_Run_Cancelled_While_Queued_Never_Reaches_The_Orchestrator_And_Cannot_Be_Sent_Later()
    {
        await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));
        await DeployAsync(DataPlan("q", "n"));
        var p = _deployments.Enqueue("p");
        await RunningAsync(p.RunId);
        var q = _deployments.Enqueue("q");

        _deployments.Cancel(q.RunId);
        _deployments.Cancel("run-unknown");

        var cancelled = Journaled(q.RunId);
        Assert.Equal((RunState.Cancelled, "Cancelled while queued."), (cancelled.State, cancelled.Refusal));
        _runs.End(p.RunId, RunOutcome.Succeeded);
        await FinishedAsync(p.RunId);
        await Task.Delay(300);
        Assert.False(_runs.Saw(q.RunId));
        Assert.Empty(_deployments.Queue);
    }

    [Fact]
    public async Task A_Pipeline_With_A_Run_Queued_Or_In_Flight_Is_Neither_Deployed_Again_Nor_Removed()
    {
        await ReplicaAsync("n");
        var plan = DataPlan("p", "n");
        await DeployAsync(plan);
        var run = _deployments.Enqueue("p");
        await RunningAsync(run.RunId);

        var again = await Assert.ThrowsAsync<ConflictException>(() => DeployAsync(plan));
        Assert.Equal("A run of 'p' is queued or in flight.", again.Message);
        var removed = await Assert.ThrowsAsync<ConflictException>(() => _deployments.UndeployAsync("p", CancellationToken.None));
        Assert.Equal("A run of 'p' is queued or in flight.", removed.Message);

        _runs.End(run.RunId, RunOutcome.Succeeded);
        await FinishedAsync(run.RunId);
        await DeployAsync(plan);
        await _deployments.UndeployAsync("p", CancellationToken.None);
        var unknown = await Assert.ThrowsAsync<ConflictException>(() => _deployments.UndeployAsync("p", CancellationToken.None));
        Assert.Equal("'p' is not deployed.", unknown.Message);
    }

    [Fact]
    public async Task A_Pipeline_Whose_Run_Waits_In_The_Queue_Is_Not_Deployed_Again()
    {
        await ReplicaAsync("n");
        var plan = DataPlan("q", "n");
        await DeployAsync(DataPlan("p", "n"));
        await DeployAsync(plan);
        var holder = _deployments.Enqueue("p");
        await RunningAsync(holder.RunId);
        var waiting = _deployments.Enqueue("q");

        var ex = await Assert.ThrowsAsync<ConflictException>(() => DeployAsync(plan));

        Assert.Equal("A run of 'q' is queued or in flight.", ex.Message);
        _runs.End(holder.RunId, RunOutcome.Succeeded);
        await RunningAsync(waiting.RunId);
        _runs.End(waiting.RunId, RunOutcome.Succeeded);
        await Task.WhenAll(FinishedAsync(holder.RunId), FinishedAsync(waiting.RunId));
    }

    [Fact]
    public async Task A_Deployment_That_Fails_Is_Not_Kept_And_The_Plan_Deployed_Before_Is_The_Active_One_Again()
    {
        await ReplicaAsync("n");
        await ReplicaAsync("m");
        await DeployAsync(DataPlan("p", "n"));
        _link.Answer = new FragmentCheck(false, "no");

        await Assert.ThrowsAsync<ConflictException>(() => DeployAsync(DataPlan("q", "m")));

        Assert.Equal("plan-p", _deployments.ActivePlan!.Id);
        Assert.Null(_deployments.DeployedPlanId("q"));
        Assert.Equal("p", Assert.Single(await _deployments.DeploymentsAsync()).PipelineId);
    }

    [Fact]
    public async Task A_Plan_That_Cannot_Be_Deployed_Is_Refused_Saying_Why_Before_Any_Replica_Is_Asked()
    {
        var bricks = DataPlan("p", "n");
        var failing = bricks with { Errors = ["a cut is missing"], FlowRejection = "no flow" };
        var edge = new PlannedEdge("p@n", "out", "p@n", "in", ["crm"], ["crm"], true);
        var refused = bricks with { Edges = [edge] };

        var errors = await Assert.ThrowsAsync<ConflictException>(() => DeployAsync(failing));
        Assert.Equal("This plan cannot be deployed: a cut is missing no flow", errors.Message);
        var matrix = await Assert.ThrowsAsync<ConflictException>(() => DeployAsync(refused));
        Assert.Equal("Plan rejected, edge(s) not permitted by the flow control matrix: p@n.out -> p@n.in (crm -> crm)", matrix.Message);
        Assert.Empty(_link.Killed);
    }

    [Fact]
    public async Task A_Run_Sent_While_Its_Deployment_Is_Being_Made_Waits_For_It_And_Ends_When_Cancelled()
    {
        await ReplicaAsync("n");
        _link.CheckGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var deploying = DeployAsync(DataPlan("p", "n"));
        await UntilAsync(() => _deployments.ActivePlan is not null, "the deployment to be registered");

        var first = _deployments.Enqueue("p");
        var second = _deployments.Enqueue("p");
        await Task.Delay(300);
        Assert.False(_runs.Saw(first.RunId));

        _deployments.Cancel(first.RunId);
        await FinishedAsync(first.RunId);
        var ended = Journaled(first.RunId);
        Assert.Equal((RunState.Cancelled, "Cancelled while queued."), (ended.State, ended.Refusal));
        Assert.False(_runs.Saw(first.RunId));

        _link.CheckGate.SetResult(new FragmentCheck(true));
        await deploying;
        await RunningAsync(second.RunId);
        Assert.True(_runs.Saw(second.RunId));
        _runs.End(second.RunId, RunOutcome.Succeeded);
        await FinishedAsync(second.RunId);
    }

    [Fact]
    public async Task A_Run_Sent_To_A_Deployment_That_Failed_Is_Refused_Saying_Why()
    {
        await ReplicaAsync("n");
        _link.CheckGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var deploying = DeployAsync(DataPlan("p", "n"));
        await UntilAsync(() => _deployments.ActivePlan is not null, "the deployment to be registered");
        var run = _deployments.Enqueue("p");
        // The run must have read the deployment, still being made, before the check fails and the deployment is forgotten.
        await Task.Delay(1000);

        _link.CheckGate.SetResult(new FragmentCheck(false, "no thanks"));
        await Assert.ThrowsAsync<ConflictException>(() => deploying);
        await FinishedAsync(run.RunId);

        var refused = Journaled(run.RunId);
        Assert.Equal(RunState.Refused, refused.State);
        Assert.StartsWith("'p' is not ready: failed ", refused.Refusal);
        Assert.Contains("refused by n: no thanks", refused.Refusal);
        Assert.False(_runs.Saw(run.RunId));
    }

    [Fact]
    public async Task The_Plan_Deployed_Last_Is_The_Active_One_And_Falls_Back_When_It_Is_Removed()
    {
        await ReplicaAsync("n");
        await ReplicaAsync("m");
        Assert.Null(_deployments.ActivePlan);

        await DeployAsync(DataPlan("p", "n"));
        await DeployAsync(DataPlan("q", "m"));
        Assert.Equal("plan-q", _deployments.ActivePlan!.Id);
        Assert.Equal(("plan-p", "plan-q"), (_deployments.DeployedPlanId("p"), _deployments.DeployedPlanId("q")));
        Assert.Equal("q", _deployments.PlanOf("q")!.PipelineId);
        Assert.Null(_deployments.DeployedPlanId("x"));

        await _deployments.UndeployAsync("q", CancellationToken.None);
        Assert.Equal("plan-p", _deployments.ActivePlan!.Id);
        await _deployments.UndeployAsync("p", CancellationToken.None);
        Assert.Null(_deployments.ActivePlan);
        Assert.Null(_deployments.PlanOf("p"));
    }

    [Fact]
    public async Task A_Fragment_Is_Killed_On_Every_Replica_Of_Its_Node_And_Only_When_A_Plan_Deployed_Names_It()
    {
        var first = await ReplicaAsync("n");
        var second = await ReplicaAsync("n");
        await DeployAsync(DataPlan("p", "n"));

        var unknown = await Assert.ThrowsAsync<ConflictException>(() => _deployments.KillAsync("x@n", CancellationToken.None));
        Assert.Equal("'x@n' is not a fragment of a deployed plan.", unknown.Message);
        await _deployments.KillAsync("p@n", CancellationToken.None);
        Assert.Equal(new[] { first, second }.OrderBy(k => k.Instance), _link.Killed.Select(k => k.Replica).OrderBy(k => k.Instance));
        Assert.All(_link.Killed, killed => Assert.Equal("p@n", killed.Fragment));

        foreach (var connection in _connectionsOf["n"]) _registry.Disconnected(connection);
        var offline = await Assert.ThrowsAsync<ConflictException>(() => _deployments.KillAsync("p@n", CancellationToken.None));
        Assert.Equal("n is offline.", offline.Message);
    }

    /// <summary>What the pages read: the events of a deployment and of a run, in the words the JSON gives them.</summary>
    [Fact]
    public async Task The_Events_Of_A_Deployment_And_Of_A_Run_Carry_The_States_As_Words()
    {
        using var host = new CoordinatorHost();
        using var started = host.CreateClient();
        var json = host.Services.GetRequiredService<IOptions<Microsoft.AspNetCore.Http.Json.JsonOptions>>().Value.SerializerOptions;
        var (_, live, unsubscribe) = _bus.Subscribe();
        try
        {
            await ReplicaAsync("n");
            await DeployAsync(DataPlan("p", "n"));
            var run = _deployments.Enqueue("p");
            await RunningAsync(run.RunId);
            _runs.End(run.RunId, RunOutcome.Failed);
            await FinishedAsync(run.RunId);
            await _deployments.UndeployAsync("p", CancellationToken.None);
            _link.Answer = new FragmentCheck(false, "no");
            await Assert.ThrowsAsync<ConflictException>(() => DeployAsync(DataPlan("q", "n")));
            await Task.Delay(300);

            var events = new List<ServerEvent>();
            while (live.TryRead(out var evt)) events.Add(evt);
            string StateOf(ServerEvent e) => JsonSerializer.SerializeToElement(e.Data, json).GetProperty("state").GetString()!;

            Assert.Equal(["deploying", "ready", "undeployed", "deploying", "failed"],
                events.Where(e => e.Type == "deploy").Select(StateOf));
            Assert.Equal(["queued", "running", "failed"], events.Where(e => e.Type == "run").Select(StateOf));
            var published = events.Where(e => e.Type == "deployments").ToList();
            Assert.NotEmpty(published);
            var states = published.SelectMany(e => JsonSerializer.SerializeToElement(e.Data, json).EnumerateArray())
                .Where(d => d.GetProperty("pipelineId").GetString() == "p").Select(d => d.GetProperty("state").GetString()!).ToList();
            Assert.NotEmpty(states);
            Assert.All(states, state => Assert.Contains(state, new[] { "deploying", "ready" }));
        }
        finally
        {
            unsubscribe();
        }
    }
}
