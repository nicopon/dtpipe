using System.Collections.Concurrent;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Models;
using TransportR.Services;
using Xunit;
using DtPipe.Tests.Shared;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The cursor of a pipeline between runs, with a scripted orchestrator whose runs the test ends: it is read when a run starts and
/// kept only at the verdict of the whole run, a launch beside a run is refused once a cursor exists, and a brick that changed since
/// the value was kept refuses the next run by name. One data node <c>n</c> holds a source brick and a sink brick that tracks <c>id</c>.
/// </summary>
public sealed class CursorRunTests : IDisposable
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);
    private const string SinkJson = """{"output":"sqlite:b","cursor":"id"}""";

    private sealed class Link : INodeLink
    {
        public Task<FragmentCheck> CheckAsync(ReplicaKey replica, FragmentSpec spec, CancellationToken ct) => Task.FromResult(new FragmentCheck(true));
        public Task<int> KillAsync(ReplicaKey replica, string fragment, CancellationToken ct) => Task.FromResult(0);
        public Task<LaunchResult> LaunchAsync(ReplicaKey replica, LaunchRequest request, CancellationToken ct) => throw new NotSupportedException();
        public Task<bool> WireAsync(ReplicaKey replica, string runId, string fragment, string alias, string transferId, CancellationToken ct) => throw new NotSupportedException();
        public Task<bool> CancelAsync(ReplicaKey replica, string runId, string fragment, CancellationToken ct) => throw new NotSupportedException();
    }

    private sealed class ScriptedRuns : IRunOrchestrator
    {
        public ConcurrentQueue<RunSpec> Specs { get; } = new();
        private readonly ConcurrentDictionary<string, TaskCompletionSource<RunResult>> _ends = new();

        public Task<RunResult> RunAsync(RunSpec spec, CancellationToken ct = default)
        {
            Specs.Enqueue(spec);
            var end = _ends.GetOrAdd(spec.RunId, _ => new(TaskCreationOptions.RunContinuationsAsynchronously));
            ct.Register(() => end.TrySetResult(Result(RunOutcome.Cancelled, null)));
            return end.Task;
        }

        public Task<ExitedAck> OnExitedAsync(ReplicaKey replica, FragmentExit exit) => throw new NotSupportedException();

        public void End(string runId, RunOutcome outcome, string? cursor) =>
            _ends.GetOrAdd(runId, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult(Result(outcome, cursor));
    }

    private static RunResult Result(RunOutcome outcome, string? cursor) => new(
        outcome, null, [],
        cursor is null
            ? new Dictionary<string, FragmentExitReport>()
            : new Dictionary<string, FragmentExitReport>
            {
                ["p@n"] = new("p@n", 0, null, null, new Dictionary<string, long>(), new Dictionary<string, CursorReport> { ["snk"] = new("id", "integer", cursor) }),
            },
        []);

    private sealed class RefusingCursorStore : ICursorStore
    {
        public IReadOnlyList<StoredCursor> Load() => [];
        public void Save(string pipeline, string alias, string document) => throw new StoreUnavailableException("the cursors");
        public void Delete(string pipeline, string alias) => throw new StoreUnavailableException("the cursors");
    }

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "cursor-" + Guid.NewGuid().ToString("N"));
    private readonly RightsStore _rights;
    private readonly ReplicaRegistry _registry = new();
    private readonly InMemoryStateStore _state = new();
    private readonly NodeInventory _inventory;
    private readonly ScriptedRuns _runs = new();
    private readonly RunJournal _journal;
    private readonly DeploymentManager _deployments;
    private int _connections;

    public CursorRunTests() : this(null) { }

    private CursorRunTests(ICursorStore? cursors)
    {
        Directory.CreateDirectory(_dir);
        var seed = Path.Combine(_dir, "seed.json");
        File.WriteAllText(seed, """{ "matrix": {}, "identities": [ { "clientId": "node-n", "name": "n", "description": "", "groups": ["crm"] } ] }""");
        var options = new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, "state"), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
        };
        _rights = MemoryRightsStore.Rights(options);
        _inventory = new NodeInventory(_registry);
        _journal = new RunJournal(MemoryRunStore.At(options.StateDir), NullLogger<RunJournal>.Instance);
        _deployments = TestDeployments.Create(
            _inventory, new Link(), _runs, new ReplicaAdmission(_registry, _state), new RightsFlowControl(_rights), _rights, new EventBus(),
            _journal, options, new FakeJobs(), cursors: cursors);
    }

    public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

    private static BrickInfo Source() => TestBricks.Announced("src", BrickKind.Source, """{"input":"sqlite:a"}""", "source");

    private static BrickInfo Sink(string json = SinkJson) => TestBricks.Announced("snk", BrickKind.Sink, json, "sink");

    private async Task AnnounceAsync(BrickInfo sink)
    {
        var client = Guid.NewGuid();
        await _state.SaveClientAsync(new ClientRegistration(client, "data-n", ["crm"], null, null, DateTime.UtcNow, Subject: "node-n"));
        var announcement = new NodeAnnouncement("n", "", [], NodeRole.Data, [Source(), sink], InstanceId: Guid.NewGuid(), DataPlaneClientId: client);
        _registry.Announce($"c-{Interlocked.Increment(ref _connections)}", "node-n", announcement, ["crm"], () => { });
        _inventory.Announced("n", "node-n", announcement, ["crm"]);
    }

    /// <summary>The pipeline <c>p</c>: the source <c>src</c> feeds the sink <c>snk</c>, both on <c>n</c>.</summary>
    private static DistributionPlan Plan()
    {
        FragmentUnit[] units = [new("src", FragmentUnitKind.Source, "src"), new("snk", FragmentUnitKind.Sink, "snk", "src")];
        var fragment = new PlannedFragment(
            "p@n", "n", ["crm"], "", FragmentJob.VersionOf(units, FragmentJob.VersionsOver([Source(), Sink()])), [], ["src", "snk"], FragmentForm.Bricks, Composition: units);
        UnitView[] views =
        [
            new("src", "src", new BranchView("src", "brick:n/src", null, [], [], null, [], [], []), [], "n", "n"),
            new("snk", "snk", new BranchView("snk", null, "brick:n/snk", ["src"], [], null, [], [], []), ["src"], "n", "n"),
        ];
        return new DistributionPlan("plan-p", "p", views, [fragment], [], [], [], null);
    }

    private async Task DeployAsync()
    {
        await AnnounceAsync(Sink());
        await _deployments.DeployAsync(Plan(), "test", CancellationToken.None);
    }

    private static async Task UntilAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException(what);
            await Task.Delay(10);
        }
    }

    private async Task<RunView> RunToEndAsync(RunOutcome outcome, string? reported)
    {
        var run = _deployments.Enqueue("p");
        await UntilAsync(() => _deployments.Queue.Any(r => r.RunId == run.RunId && r.State == RunState.Running), $"{run.RunId} to run");
        _runs.End(run.RunId, outcome, reported);
        await UntilAsync(() => _journal.List().Any(r => r.RunId == run.RunId), $"{run.RunId} to be journaled");
        return _journal.List().Single(r => r.RunId == run.RunId);
    }

    private CursorLaunch? StartedFrom(string runId) => _runs.Specs.Single(s => s.RunId == runId).Fragments.Single().Cursor;

    [Fact]
    public async Task The_Second_Run_Starts_From_The_Value_The_First_Kept()
    {
        await DeployAsync();

        var first = await RunToEndAsync(RunOutcome.Succeeded, "42");
        Assert.Null(StartedFrom(first.RunId));
        Assert.Equal([new CursorOutcome("snk", true, "42", null)], first.Cursors);

        var second = await RunToEndAsync(RunOutcome.Succeeded, "57");
        Assert.Equal(new CursorLaunch("id", "integer", "42"), StartedFrom(second.RunId));
        var kept = Assert.Single(_deployments.Cursors.Of("p"));
        Assert.Equal(("57", second.RunId), (kept.Value, kept.RunId));
    }

    [Theory]
    [InlineData(RunOutcome.Failed)]
    [InlineData(RunOutcome.Cancelled)]
    public async Task A_Run_That_Did_Not_Succeed_As_A_Whole_Advances_Nothing(RunOutcome outcome)
    {
        await DeployAsync();
        await RunToEndAsync(RunOutcome.Succeeded, "42");

        var lost = await RunToEndAsync(outcome, "99");

        Assert.Null(lost.Cursors);
        Assert.Equal("42", Assert.Single(_deployments.Cursors.Of("p")).Value);
        var next = await RunToEndAsync(RunOutcome.Succeeded, "57");
        Assert.Equal("42", StartedFrom(next.RunId)!.Value);
    }

    [Fact]
    public async Task A_Launch_Beside_A_Run_Is_Refused_Once_A_Cursor_Exists_And_Not_Before()
    {
        await DeployAsync();

        var firstEver = _deployments.Enqueue("p");
        await UntilAsync(() => _deployments.Queue.Any(r => r.RunId == firstEver.RunId && r.State == RunState.Running), "the first run");
        var waiting = _deployments.Enqueue("p");
        Assert.Equal(RunState.Queued, waiting.State);
        _runs.End(firstEver.RunId, RunOutcome.Succeeded, "42");
        await UntilAsync(() => _deployments.Queue.Any(r => r.RunId == waiting.RunId && r.State == RunState.Running), "the waiting run");

        var refused = Assert.Throws<ConflictException>(() => _deployments.Enqueue("p"));
        Assert.Contains(waiting.RunId, refused.Message);
        Assert.Contains("advances its cursor", refused.Message);
        Assert.Equal("42", StartedFromOrNull(waiting.RunId)!.Value);
        Assert.Throws<ConflictException>(() => _deployments.ResetCursor("p", "snk"));
        _runs.End(waiting.RunId, RunOutcome.Succeeded, "50");
        await UntilAsync(() => _journal.List().Any(r => r.RunId == waiting.RunId), "the end");
    }

    private CursorLaunch? StartedFromOrNull(string runId) => _runs.Specs.SingleOrDefault(s => s.RunId == runId)?.Fragments.Single().Cursor;

    [Fact]
    public async Task A_Brick_That_Changed_Since_The_Value_Was_Kept_Refuses_The_Run_By_Name()
    {
        await DeployAsync();
        await RunToEndAsync(RunOutcome.Succeeded, "42");

        await AnnounceAsync(Sink("""{"output":"sqlite:b","cursor":"updated_at"}"""));
        var run = _deployments.Enqueue("p");
        await UntilAsync(() => _journal.List().Any(r => r.RunId == run.RunId), "the refusal");

        var refused = _journal.List().Single(r => r.RunId == run.RunId);
        Assert.Equal(RunState.Refused, refused.State);
        Assert.Contains("sink brick 'n/snk' changed since the cursor of 'snk' was kept", refused.Refusal);
        Assert.Contains("reset the cursor", refused.Refusal);
        Assert.DoesNotContain(_runs.Specs, s => s.RunId == run.RunId);

        Assert.True(_deployments.ResetCursor("p", "snk"));
        var restarted = await RunToEndAsync(RunOutcome.Succeeded, "7");
        Assert.Null(StartedFrom(restarted.RunId));
    }

    [Fact]
    public async Task A_Store_That_Refuses_The_Write_Leaves_The_Run_Succeeded_And_Says_The_Cursor_Did_Not_Advance()
    {
        using var refusing = new CursorRunTests(new RefusingCursorStore());
        await refusing.DeployAsync();

        var run = await refusing.RunToEndAsync(RunOutcome.Succeeded, "42");

        Assert.Equal(RunState.Succeeded, run.State);
        var outcome = Assert.Single(run.Cursors!);
        Assert.False(outcome.Kept);
        Assert.Contains("did not advance", outcome.Reason);
        Assert.Empty(refusing._deployments.Cursors.Of("p"));
    }

    [Fact]
    public async Task A_Value_Out_Of_The_Rule_Is_Not_Kept()
    {
        await DeployAsync();

        var run = await RunToEndAsync(RunOutcome.Succeeded, "1'; drop table t; --");

        Assert.Equal(RunState.Succeeded, run.State);
        Assert.False(Assert.Single(run.Cursors!).Kept);
        Assert.Empty(_deployments.Cursors.Of("p"));
    }

    [Fact]
    public async Task A_Person_Sets_A_Value_Before_Any_Run_And_The_First_Run_Starts_From_It()
    {
        await DeployAsync();

        _deployments.SetCursor("p", "snk", "integer", "100", "ann@x.test");
        var run = await RunToEndAsync(RunOutcome.Succeeded, "120");

        Assert.Equal("100", StartedFrom(run.RunId)!.Value);
        var kept = Assert.Single(_deployments.Cursors.Of("p"));
        Assert.Equal(("120", "id"), (kept.Value, kept.Column));
        Assert.NotNull(kept.Basis);
    }
}
