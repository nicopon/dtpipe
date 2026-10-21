using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Models;
using Xunit;
using DtPipe.Tests.Shared;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The health of an armed pipeline, read on the same harness as the deployments: a status for each finding, what is kept between two
/// reads, and the verdict of a mode that is asked once in the background.
/// </summary>
public sealed partial class ArmedDeploymentTests
{
    private static readonly TimeSpan MaxAge = TimeSpan.FromMinutes(5);

    /// <summary>What the library holds of each pipeline now; by default the job and bindings that were armed.</summary>
    private sealed class FakeHeads : ILibraryHeads
    {
        public Dictionary<string, LibraryHead?> Heads { get; } = [];
        public bool Armed { get; set; } = true;

        public Task<LibraryHead?> HeadAsync(string pipelineId, CancellationToken ct) =>
            Task.FromResult(Heads.TryGetValue(pipelineId, out var head) ? head : new LibraryHead(Commit, PipelineLibrary.Sha(Job), null));
    }

    /// <summary>A verifier whose answer is held until a test lets it go, counting how often a node was asked.</summary>
    private sealed class Held(BrickCatalog bricks, NodeRequests nodes, BindingVerdict verdict) : BindingVerifier(bricks, nodes)
    {
        public readonly TaskCompletionSource Gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Count;

        protected override async Task<BindingVerdict> AskAsync(BrickView brick, BindingPin pin, string mode)
        {
            Interlocked.Increment(ref Count);
            await Gate.Task;
            return verdict;
        }
    }

    private Held HoldVerdict(BindingVerdict verdict)
    {
        Held? made = null;
        _verifier = (bricks, nodes) => made = new Held(bricks, nodes, verdict);
        Restart();
        return made!;
    }

    private async Task ArmedHealthyAsync(Binding? binding = null)
    {
        await NodeAsync("n", "crm", NodeRole.Data, binding is null ? null : Changed());
        await NodeAsync("r1", "crm", NodeRole.Runner);
        await _deployments.ArmAsync(binding is null ? Armed() : ArmedWith(binding), Job, CancellationToken.None);
        await SettledAsync();
    }

    /// <summary>Waits until the deployments' own publications, which follow an arming in the background, have all been made.</summary>
    private async Task SettledAsync()
    {
        long seen;
        do
        {
            seen = _bus.CountOf(PipelineHealth.Subjects);
            await Task.Delay(60);
        }
        while (_bus.CountOf(PipelineHealth.Subjects) != seen);
    }

    private async Task<PipelineHealthView> ViewAsync() => Assert.Single((await _health.ReadAsync("p", CancellationToken.None)).Pipelines);

    /// <summary>Reads again until the status is not the one a background ask is going to change.</summary>
    /// <summary>Waits for a state that a background task reaches, which no view reports.</summary>
    private static async Task UntilAsync(Func<bool> reached)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (!reached())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("The background task did not reach the state awaited.");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    private async Task<PipelineHealthView> ViewWhenAsync(Func<PipelineHealthView, bool> done)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (true)
        {
            var view = await ViewAsync();
            if (done(view)) return view;
            if (DateTime.UtcNow > deadline) throw new TimeoutException(string.Join(", ", view.Causes.Select(c => c.Code)));
            await Task.Delay(20);
        }
    }

    private static HealthCause Cause(PipelineHealthView view, string code) => Assert.Single(view.Causes, c => c.Code == code);

    /// <summary>Announces one more replica of the data node <c>n</c>, whose data plane client is registered or not.</summary>
    private async Task<ClientRegistration> ReplicaOfNAsync(bool registered, params BrickInfo[] bricks)
    {
        var client = Guid.NewGuid();
        var registration = new ClientRegistration(client, "data-n-" + client.ToString("N")[..4], ["crm"], null, null, DateTime.UtcNow, Subject: "node-n");
        if (registered) await _state.SaveClientAsync(registration);
        var announcement = new NodeAnnouncement(
            "n", "", [], NodeRole.Data, bricks.Length > 0 ? bricks : [Brick()],
            InstanceId: Guid.NewGuid(), DataPlaneClientId: client);
        _registry.Announce($"c-n-{Interlocked.Increment(ref _connections)}", "node-n", announcement, ["crm"], () => { });
        _inventory.Announced("n", "node-n", announcement, ["crm"]);
        return registration;
    }

    // ---- ok, and what is only worth knowing ----

    [Fact]
    public async Task A_Pipeline_That_Can_Be_Launched_Is_Ok_And_Each_Fragment_Counts_Its_Replicas()
    {
        await ArmedHealthyAsync();

        var report = await _health.ReadAsync(CancellationToken.None);

        Assert.Equal(HealthStatus.Ok, report.Status);
        var view = Assert.Single(report.Pipelines);
        Assert.Equal(("p", HealthStatus.Ok, Commit, Commit), (view.Id, view.Status, view.Commit, view.LibraryCommit));
        Assert.All(view.Causes, c => Assert.Equal(HealthSeverity.Info, c.Severity));
        Assert.Equal([("p@n", 1, 1), ("p@r1", 1, 1)], view.Fragments.OrderBy(f => f.Name).Select(f => (f.Name, f.Replicas, f.Eligible)));
    }

    [Fact]
    public async Task What_Is_Only_Worth_Knowing_Changes_No_Status()
    {
        await ArmedHealthyAsync();
        var run = _deployments.Enqueue("p");
        await RanAsync(run);
        _heads.Heads["p"] = new LibraryHead("fedcba9876543210", PipelineLibrary.Sha("other: {input: x}\n"), null);

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Ok, view.Status);
        Assert.Equal("fedcba9876543210", view.LibraryCommit);
        Assert.All(view.Causes, c => Assert.Equal(HealthSeverity.Info, c.Severity));
        Cause(view, HealthCauses.LibraryNewer);
        Assert.Contains(run.RunId, Cause(view, HealthCauses.LastRun).Message);
    }

    [Fact]
    public async Task A_Pipeline_Removed_From_The_Library_Stays_Armed_And_Launchable_And_Says_So()
    {
        await ArmedHealthyAsync();
        _heads.Heads["p"] = null;

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Ok, view.Status);
        Assert.Null(view.LibraryCommit);
        Assert.Equal(HealthSeverity.Info, Cause(view, HealthCauses.LibraryRemoved).Severity);
    }

    [Fact]
    public async Task A_Tolerant_Brick_That_Changed_Is_Told_And_Leaves_The_Status_Ok_With_No_Node_Asked()
    {
        await ArmedHealthyAsync(new Binding("n", "src", "tolerant", new BindingPin("an-older-version")));

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Ok, view.Status);
        Assert.Equal(HealthSeverity.Info, Cause(view, HealthCauses.BrickChanged).Severity);
    }

    // ---- warning ----

    [Fact]
    public async Task A_Replica_That_Cannot_Run_The_Fragment_While_Another_Can_Is_A_Warning()
    {
        await ArmedHealthyAsync();
        await ReplicaOfNAsync(registered: false);

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Warning, view.Status);
        var degraded = Cause(view, HealthCauses.ReplicaDegraded);
        Assert.Equal(("n", "p@n", HealthSeverity.Warning), (degraded.Node, degraded.Fragment, degraded.Severity));
        Assert.Equal((2, 1), view.Fragments.Where(f => f.Name == "p@n").Select(f => (f.Replicas, f.Eligible)).Single());
    }

    [Fact]
    public async Task One_Replica_Is_Never_A_Warning()
    {
        await ArmedHealthyAsync();

        Assert.DoesNotContain((await ViewAsync()).Causes, c => c.Severity != HealthSeverity.Info);
    }

    [Fact]
    public async Task Replicas_That_Offer_Different_Bricks_Are_A_Warning()
    {
        await ArmedHealthyAsync();
        await ReplicaOfNAsync(registered: true, Brick(), TestBricks.Announced("other", BrickKind.Source, """{"input":"sqlite:z"}""", "source"));

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Warning, view.Status);
        Assert.Equal("n", Cause(view, HealthCauses.ReplicasDisagree).Node);
    }

    [Fact]
    public async Task A_Verdict_Still_Being_Asked_Is_A_Warning_And_Is_Asked_Once_In_The_Background()
    {
        var node = HoldVerdict(new BindingVerdict(BindingVerdicts.Compatible, "wider"));
        await ArmedHealthyAsync(new Binding("n", "src", "compatible", new BindingPin("an-older-version", "{}")));

        var first = await ViewAsync();
        await UntilAsync(() => node.Count == 1);
        _bus.Publish("nodes", new { });
        var second = await ViewAsync();
        // A second question would start on a pool thread: give it the time to show.
        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Warning, first.Status);
        Assert.Equal(HealthSeverity.Warning, Cause(first, HealthCauses.VerdictPending).Severity);
        Assert.Equal(HealthStatus.Warning, second.Status);
        Assert.Equal(1, node.Count);

        // The answer ends the read that said "in flight", though nothing the bus carries moved.
        node.Gate.SetResult();
        var done = await ViewWhenAsync(v => v.Causes.All(c => c.Code != HealthCauses.VerdictPending));
        Assert.Equal(HealthStatus.Ok, done.Status);
        Assert.Equal(HealthSeverity.Info, Cause(done, HealthCauses.BrickChanged).Severity);
        Assert.Equal(1, node.Count);
    }

    [Fact]
    public async Task A_Pipeline_Being_Armed_Is_A_Warning_Until_It_Is_Ready()
    {
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        var release = new TaskCompletionSource();
        _link.Hold = release.Task;
        var arming = _deployments.ArmAsync(Armed(), Job, CancellationToken.None);
        while (_deployments.Armed().Count == 0) await Task.Delay(10);

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Warning, view.Status);
        Assert.Equal(HealthSeverity.Warning, Cause(view, HealthCauses.Deploying).Severity);
        release.SetResult();
        await arming;
        await SettledAsync();
        Assert.Equal(HealthStatus.Ok, (await ViewAsync()).Status);
    }

    // ---- critical ----

    [Fact]
    public async Task A_Pipeline_That_Is_Not_Armed_Is_Critical_So_That_A_Disarming_Alerts()
    {
        var report = await _health.ReadAsync("ghost", CancellationToken.None);

        Assert.Equal(HealthStatus.Critical, report.Status);
        var view = Assert.Single(report.Pipelines);
        Assert.Equal(("ghost", HealthCauses.NotArmed), (view.Id, Assert.Single(view.Causes).Code));
    }

    [Fact]
    public async Task A_Strict_Brick_That_Changed_Is_Critical()
    {
        await ArmedHealthyAsync();
        await NodeAsync("n", "crm", NodeRole.Data, Changed());

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Critical, view.Status);
        Assert.Equal(("src", "n"), (Cause(view, BindingCauses.BrickChanged).Alias, Cause(view, BindingCauses.BrickChanged).Node));
    }

    [Fact]
    public async Task A_Node_That_Lost_Its_Identity_Or_Was_Disabled_Is_Critical()
    {
        await ArmedHealthyAsync();
        _rights.UpdateIdentity("node-n", null, null, null, enabled: false);
        Assert.Equal(BindingCauses.UnknownNode, Cause(await ViewAsync(), BindingCauses.UnknownNode).Code);
        Assert.Equal(HealthStatus.Critical, (await ViewAsync()).Status);

        _rights.RemoveIdentity("node-n");
        _bus.Publish("rights", new { });

        var view = await ViewAsync();
        Assert.Equal(HealthStatus.Critical, view.Status);
        Cause(view, BindingCauses.UnknownNode);
    }

    [Fact]
    public async Task After_A_Restart_Before_Any_Node_Announces_The_Cause_Is_The_Absence_Not_A_Brick_That_Went()
    {
        await ArmedHealthyAsync();
        Restart();

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Critical, view.Status);
        Assert.Equal(HealthSeverity.Critical, Cause(view, BindingCauses.Offline).Severity);
        Assert.DoesNotContain(view.Causes, c => c.Code is BindingCauses.NotABrick or BindingCauses.UnknownNode);
    }

    [Fact]
    public async Task A_Node_Whose_Replicas_Are_All_Gone_Is_Critical_Once_And_Not_Twice()
    {
        await ArmedHealthyAsync();
        _registry.Disconnected(_registry.Replicas("node-n").Single().ConnectionId!);

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Critical, view.Status);
        Cause(view, BindingCauses.Offline);
        Assert.DoesNotContain(view.Causes, c => c.Code == LaunchCauses.NoEligibleReplica);
    }

    [Fact]
    public async Task A_Data_Node_Whose_Replica_Cannot_Run_Its_Fragment_Is_Critical()
    {
        await NodeAsync("r1", "crm", NodeRole.Runner);
        await ReplicaOfNAsync(registered: true);
        await _deployments.ArmAsync(Armed(), Job, CancellationToken.None);
        Restart();
        await NodeAsync("r1", "crm", NodeRole.Runner);
        await ReplicaOfNAsync(registered: false);

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Critical, view.Status);
        Assert.Equal(("n", "p@n"), (Cause(view, LaunchCauses.NoEligibleReplica).Node, Cause(view, LaunchCauses.NoEligibleReplica).Fragment));
    }

    [Fact]
    public async Task No_Runner_Online_For_A_Job_That_Needs_One_Is_Critical()
    {
        await ArmedHealthyAsync();
        Restart();
        await NodeAsync("n", "crm", NodeRole.Data);

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Critical, view.Status);
        Cause(view, LaunchCauses.NoRunner);
    }

    [Fact]
    public async Task A_Matrix_That_Forbids_An_Edge_Is_Critical()
    {
        await ArmedHealthyAsync();
        _rights.SetMatrix(new Dictionary<string, List<string>>());

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Critical, view.Status);
        Cause(view, LaunchCauses.FlowRejected);
    }

    [Fact]
    public async Task A_Job_The_Library_Cannot_Give_At_The_Armed_Commit_Is_Critical()
    {
        await ArmedHealthyAsync();
        _jobs.Jobs.Clear();

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Critical, view.Status);
        Cause(view, HealthCauses.JobUnreadable);
    }

    [Fact]
    public async Task A_Pipeline_Whose_Health_Cannot_Be_Read_Is_Critical_And_The_Others_Are_Still_Told()
    {
        await ArmedHealthyAsync();
        _jobs.Fault = new IOException("git failed");

        var view = await ViewAsync();

        Assert.Equal(HealthStatus.Critical, view.Status);
        Assert.Contains("git failed", Cause(view, HealthCauses.CheckFailed).Message);
    }

    [Fact]
    public async Task A_Verdict_That_Breaks_Is_Critical_Once_The_Node_Answered()
    {
        var node = HoldVerdict(new BindingVerdict(BindingVerdicts.Breaking, "id: int64 → int32"));
        await ArmedHealthyAsync(new Binding("n", "src", "contract", new BindingPin("an-older-version", "{}")));
        Assert.Equal(HealthStatus.Warning, (await ViewAsync()).Status);

        node.Gate.SetResult();
        var view = await ViewWhenAsync(v => v.Status == HealthStatus.Critical);

        var cause = Cause(view, HealthCauses.VerdictBreaking);
        Assert.Equal(("src", "n"), (cause.Alias, cause.Node));
        Assert.Contains("id: int64 → int32", cause.Message);
    }

    [Fact]
    public async Task A_Verdict_The_Node_Could_Not_Give_Is_Critical_And_Is_Not_Asked_Again_Within_The_Cache_Age()
    {
        var node = HoldVerdict(new BindingVerdict(BindingVerdicts.Unread, "n did not answer within 40s."));
        node.Gate.SetResult();
        await ArmedHealthyAsync(new Binding("n", "src", "contract", new BindingPin("an-older-version", "{}")));

        var view = await ViewWhenAsync(v => v.Status == HealthStatus.Critical);
        _bus.Publish("nodes", new { });
        var again = await ViewAsync();

        Assert.Equal(HealthSeverity.Critical, Cause(view, HealthCauses.VerdictUnread).Severity);
        Assert.Equal(HealthStatus.Critical, again.Status);
        Assert.Equal(1, node.Count);
    }

    // ---- what is kept ----

    [Fact]
    public async Task Two_Reads_With_Nothing_Published_Compute_Once()
    {
        await ArmedHealthyAsync();
        var before = _health.Computed;

        var first = await _health.ReadAsync(CancellationToken.None);
        var second = await _health.ReadAsync(CancellationToken.None);

        Assert.Same(first, second);
        Assert.Equal(before + 1, _health.Computed);
    }

    [Fact]
    public async Task Concurrent_Reads_Share_One_Computation()
    {
        await ArmedHealthyAsync();
        var before = _health.Computed;

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => _health.ReadAsync(CancellationToken.None)));

        Assert.Equal(before + 1, _health.Computed);
    }

    [Theory]
    [InlineData("nodes")]
    [InlineData("rights")]
    [InlineData("library")]
    [InlineData("deploy")]
    [InlineData("deployments")]
    public async Task What_The_Server_Publishes_About_The_State_Ends_The_Kept_Read(string subject)
    {
        await ArmedHealthyAsync();
        await _health.ReadAsync(CancellationToken.None);
        var before = _health.Computed;

        _bus.Publish(subject, new { });
        await _health.ReadAsync(CancellationToken.None);

        Assert.Equal(before + 1, _health.Computed);
    }

    [Fact]
    public async Task What_The_State_Does_Not_Depend_On_Ends_Nothing()
    {
        await ArmedHealthyAsync();
        await _health.ReadAsync(CancellationToken.None);
        var before = _health.Computed;

        _bus.Publish("log", new { });
        _bus.Publish("run", new { });
        await _health.ReadAsync(CancellationToken.None);

        Assert.Equal(before, _health.Computed);
    }

    [Fact]
    public async Task A_Read_Is_Never_Kept_Longer_Than_The_Maximum_Age()
    {
        await ArmedHealthyAsync();
        await _health.ReadAsync(CancellationToken.None);
        var before = _health.Computed;

        _time.Advance(MaxAge - TimeSpan.FromSeconds(1));
        await _health.ReadAsync(CancellationToken.None);
        Assert.Equal(before, _health.Computed);

        _time.Advance(TimeSpan.FromSeconds(2));
        await _health.ReadAsync(CancellationToken.None);
        Assert.Equal(before + 1, _health.Computed);
    }

    [Fact]
    public async Task A_Read_With_A_Replica_That_Cannot_Run_A_Fragment_Is_Kept_Only_Briefly_Since_Its_Client_May_Register_With_No_Event()
    {
        await ArmedHealthyAsync();
        var late = await ReplicaOfNAsync(registered: false);
        Assert.Equal(HealthStatus.Warning, (await ViewAsync()).Status);
        var before = _health.Computed;

        await _state.SaveClientAsync(late);
        _time.Advance(PipelineHealth.Unsettled - TimeSpan.FromSeconds(1));
        await _health.ReadAsync(CancellationToken.None);
        Assert.Equal(before, _health.Computed);

        _time.Advance(TimeSpan.FromSeconds(2));
        var view = await ViewAsync();
        Assert.Equal(before + 1, _health.Computed);
        Assert.Equal(HealthStatus.Ok, view.Status);
    }

    [Fact]
    public async Task A_Read_Where_Every_Connected_Replica_Can_Run_Its_Fragment_Is_Kept_Up_To_The_Maximum_Age()
    {
        await ArmedHealthyAsync();
        await _health.ReadAsync(CancellationToken.None);
        var before = _health.Computed;

        _time.Advance(PipelineHealth.Unsettled + TimeSpan.FromSeconds(1));
        await _health.ReadAsync(CancellationToken.None);

        Assert.Equal(before, _health.Computed);
    }
}
