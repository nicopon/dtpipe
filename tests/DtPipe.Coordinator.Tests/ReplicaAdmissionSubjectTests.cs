using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using DtPipe.Tests.Shared;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// A run is placed on a replica of its node's identity and on no other, against a hub that attests subjects: each
/// replica carries a token, and a replica of another identity that announces the node's name is neither chosen nor
/// counted. What a replica says of a run is heard only when the run placed it there.
/// </summary>
public class ReplicaAdmissionSubjectTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private static Task<CoordinatorTestHost> StartAsync(TestJwt jwt) =>
        CoordinatorTestHost.StartAsync(_ => { }, services => services.AddSingleton(new RunOrchestratorOptions
        {
            AdmissionTimeout = TimeSpan.FromSeconds(1),
            ReadyTimeout = TimeSpan.FromSeconds(2),
            ExitTimeout = TimeSpan.FromSeconds(1),
        }), jwt);

    private static FakeNodeOptions As(TestJwt jwt, string subject, Guid? instance = null) =>
        new() { Token = jwt.Mint(subject, "test", TimeSpan.FromHours(1)), InstanceId = instance };

    /// <summary>A fragment for the replicas of <paramref name="node"/>, whose data plane clients hold the group <c>test</c>.</summary>
    private static RunFragment Fragment(string name, string? node, IReadOnlyList<string>? groups = null) =>
        new(new FragmentSpec(name, "v1", [], Yaml: "x:\n  input: generate:1\n"), node, groups ?? ["test"]);

    private static RunSpec Run(string runId, params RunFragment[] fragments) => new(runId, fragments, []);

    /// <summary>The launch a replica receives, or, when the run was refused first, the refusal.</summary>
    private static async Task<LaunchRequest> LaunchedAsync(FakeNode node, Task<RunResult> run)
    {
        var first = await Task.WhenAny(node.Launched.Task, run).WaitAsync(Wait);
        if (first == run) await run;
        return await node.Launched.Task;
    }

    [Fact]
    public async Task TheRegistryFilesAReplicaUnderTheSubjectItsConnectionAttested()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);

        await using var node = await FakeNode.StartAsync(host, "any-name-it-proposes", As(jwt, "node-legit"));

        var replica = Assert.Single(host.Host.Services.GetRequiredService<ReplicaRegistry>().Replicas());
        Assert.Equal("node-legit", replica.Key.Node);
    }

    /// <summary>A replica of another identity announces the name of the node: counted as a replica of it, it would be launched.</summary>
    [Fact]
    public async Task AReplicaOfAnotherIdentityAnnouncingTheNodesName_IsNeitherChosenNorCounted()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        await using var foreign = await FakeNode.StartAsync(host, "node-legit", As(jwt, "node-foreign"));
        await using var legit = await FakeNode.StartAsync(host, "node-legit", As(jwt, "node-legit"));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var run = orchestrator.RunAsync(Run("run-1", Fragment("frag-a", "node-legit")));
        await LaunchedAsync(legit, run);
        await legit.ExitAsync(0);
        var result = await run.WaitAsync(Wait);

        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        Assert.Empty(foreign.Launches);
    }

    /// <summary>The only replica that announces the node's name is of another identity: the node has none, and the run is refused for it.</summary>
    [Fact]
    public async Task OnlyAReplicaOfAnotherIdentityAnnouncingTheNodesName_LeavesTheNodeWithNoReplica()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        await using var foreign = await FakeNode.StartAsync(host, "node-legit", As(jwt, "node-foreign"));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var ex = await Assert.ThrowsAsync<NoEligibleReplicaException>(
            () => orchestrator.RunAsync(Run("run-1", Fragment("frag-a", "node-legit"))).WaitAsync(Wait));

        Assert.Contains("no replica of its node is online", ex.Reasons["frag-a"]);
        Assert.Empty(foreign.Launches);
    }

    /// <summary>A hub that attests subjects admits no replica whose connection attests none; one that attests none files it under the name it proposes.</summary>
    [Fact]
    public async Task AnAnnouncementThatAttestsNoSubject_IsRefused_UnlessTheHubAttestsNone()
    {
        await using var strict = await StartAsync(new TestJwt());
        await using var dev = await CoordinatorTestHost.StartAsync(_ => { });
        var announcement = new NodeAnnouncement("node-x", "", [], NodeRole.Runner, [], InstanceId: Guid.NewGuid());

        var (refused, reason) = await strict.Host.Services.GetRequiredService<ReplicaProtocol>()
            .OnAnnouncedAsync("c1", subject: null, announcement, [], () => { });
        var (filed, none) = await dev.Host.Services.GetRequiredService<ReplicaProtocol>()
            .OnAnnouncedAsync("c1", subject: null, announcement, [], () => { });

        Assert.Null(refused);
        Assert.Contains("attests no subject", reason);
        Assert.Empty(strict.Host.Services.GetRequiredService<ReplicaRegistry>().Replicas());
        Assert.Null(none);
        Assert.Equal("node-x", filed!.Value.Node);
    }

    /// <summary>
    /// A replica of another identity says, while the legitimate node runs the fragment, that the fragment exited with a
    /// local fault. The report is not the placed replica's: the coordinator answers that it did not admit it, and the
    /// run goes on and ends on what the legitimate replica says.
    /// </summary>
    [Fact]
    public async Task AReplicaOfAnotherIdentityReportingExited_DuringTheRun_DoesNotDecideTheVerdict()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        await using var foreign = await FakeNode.StartAsync(host, "node-legit", As(jwt, "node-foreign"));
        await using var legit = await FakeNode.StartAsync(host, "node-legit", As(jwt, "node-legit"));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var run = orchestrator.RunAsync(Run("run-1", Fragment("frag-a", "node-legit")));
        var launched = await LaunchedAsync(legit, run);

        // It sends the launch's own token, so that only the replica rule can refuse it.
        var answer = await foreign.ExitAsync(1, "Local", launched.RunId, "frag-a", launchToken: launched.LaunchToken);
        Assert.Equal(ExitedAck.NotAdmitted, answer);
        Assert.False(run.IsCompleted);

        Assert.Equal(ExitedAck.Accepted, await legit.ExitAsync(0));
        var result = await run.WaitAsync(Wait);
        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        Assert.Null(result.Cause);
    }

    /// <summary>
    /// The same, from a replica of the same identity that the run did not place the fragment on. Only this one shows the
    /// report is read by the replica's key and not by the subject the connection attests.
    /// </summary>
    [Fact]
    public async Task ASiblingReplicaNotPlacedOnTheRun_ReportingExited_DoesNotDecideTheVerdict()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        // The tie between two idle replicas goes to the lowest instance id: the first is the one placed.
        await using var placed = await FakeNode.StartAsync(host, "node-legit", As(jwt, "node-legit", Guid.Parse("00000000-0000-0000-0000-000000000001")));
        await using var sibling = await FakeNode.StartAsync(host, "node-legit", As(jwt, "node-legit", Guid.Parse("00000000-0000-0000-0000-000000000002")));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var run = orchestrator.RunAsync(Run("run-1", Fragment("frag-a", "node-legit")));
        var launched = await LaunchedAsync(placed, run);
        Assert.Empty(sibling.Launches);

        Assert.Equal(ExitedAck.NotAdmitted, await sibling.ExitAsync(1, "Local", launched.RunId, "frag-a", launchToken: launched.LaunchToken));
        Assert.False(run.IsCompleted);

        await placed.ExitAsync(0);
        var result = await run.WaitAsync(Wait);
        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        Assert.Null(result.Cause);
    }

    /// <summary>A replica that announces itself after the run was asked for is placed, as long as it comes before the admission timeout.</summary>
    [Fact]
    public async Task ARunWaitsForAReplicaThatAnnouncesLate_AndIsPlacedBeforeTheAdmissionTimeout()
    {
        await using var host = await CoordinatorTestHost.StartAsync(
            _ => { }, services => services.AddSingleton(new RunOrchestratorOptions { AdmissionTimeout = TimeSpan.FromSeconds(5) }));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var run = orchestrator.RunAsync(Run("run-1", TestFragments.Edgeless("late")));
        await Task.Delay(300);
        Assert.False(run.IsCompleted);
        await using var node = await FakeNode.StartAsync(host, "late");

        await LaunchedAsync(node, run);
        await node.ExitAsync(0);
        Assert.Equal(RunOutcome.Succeeded, (await run.WaitAsync(Wait)).Outcome);
    }

    /// <summary>Nothing says which node may host the fragment: a hub that attests subjects refuses the run at once, naming the fragment.</summary>
    [Fact]
    public async Task AFragmentBoundToNoNode_IsRefusedAtOnce_NamingTheFragment()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        await using var node = await FakeNode.StartAsync(host, "node-legit", As(jwt, "node-legit"));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var ex = await Assert.ThrowsAsync<UnboundFragmentException>(
            () => orchestrator.RunAsync(Run("run-1", Fragment("frag-a", node: null))).WaitAsync(Wait));

        Assert.Equal("frag-a", ex.FragmentName);
        Assert.Empty(node.Launches);
    }

    /// <summary>A fragment that names no groups would skip the check of the data plane: refused, as one that names no node.</summary>
    [Fact]
    public async Task AFragmentNamingNoGroups_IsRefusedAtOnce()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        await using var node = await FakeNode.StartAsync(host, "node-legit", As(jwt, "node-legit"));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var unbound = new RunFragment(Fragment("frag-a", "node-legit").Spec, "node-legit", Groups: null);
        await Assert.ThrowsAsync<UnboundFragmentException>(() => orchestrator.RunAsync(Run("run-1", unbound)).WaitAsync(Wait));

        Assert.Empty(node.Launches);
    }

    /// <summary>
    /// A replica announces the TransportR client of another identity. The announcement is refused when that client is
    /// registered under another subject, and its connection is closed: it would be handed the other's transfers.
    /// </summary>
    [Fact]
    public async Task AReplicaAnnouncingAClientRegisteredUnderAnotherSubject_IsRefusedAtTheAnnouncement()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        var owner = host.CreateClient(jwt.Mint("node-y", "test", TimeSpan.FromHours(1)));
        await using var _owner = owner;
        await owner.ConnectAsync();

        await using var claimant = await FakeNode.StartAsync(host, "node-x", new FakeNodeOptions
        {
            Token = jwt.Mint("node-x", "test", TimeSpan.FromHours(1)), ConnectDataPlane = false, Announce = false, AnnouncedDataPlaneClientId = owner.ClientId,
        });
        await Record.ExceptionAsync(claimant.AnnounceAsync);

        Assert.Empty(host.Host.Services.GetRequiredService<ReplicaRegistry>().Replicas("node-x"));
    }

    /// <summary>One TransportR client carries one replica: a second replica announcing the same client is refused, whatever its identity.</summary>
    [Fact]
    public async Task AReplicaAnnouncingTheClientOfAnotherReplica_IsRefusedAtTheAnnouncement()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        await using var owner = await FakeNode.StartAsync(host, "node-x", As(jwt, "node-x", Guid.Parse("00000000-0000-0000-0000-000000000001")));

        await using var claimant = await FakeNode.StartAsync(host, "node-x", new FakeNodeOptions
        {
            Token = jwt.Mint("node-x", "test", TimeSpan.FromHours(1)), ConnectDataPlane = false, Announce = false,
            AnnouncedDataPlaneClientId = owner.DataPlane!.ClientId, InstanceId = Guid.Parse("00000000-0000-0000-0000-000000000002"),
        });
        await Record.ExceptionAsync(claimant.AnnounceAsync);

        Assert.Equal(Guid.Parse("00000000-0000-0000-0000-000000000001").ToString("N"), Assert.Single(host.Host.Services.GetRequiredService<ReplicaRegistry>().Replicas("node-x")).Key.Instance);
    }

    /// <summary>
    /// The announcement is a courtesy; admission is the authority. A replica announces a client id before its owner has
    /// registered it, so the announcement passes; once the owner registers it under its own subject, the replica that
    /// claimed it is never chosen, and its node's run is refused.
    /// </summary>
    [Fact]
    public async Task AReplicaThatAnnouncedAClientBeforeItsOwnerRegisteredIt_IsNeverChosen()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        var owner = host.CreateClient(jwt.Mint("node-y", "test", TimeSpan.FromHours(1)));
        await using var _owner = owner;

        await using var claimant = await FakeNode.StartAsync(host, "node-x", new FakeNodeOptions
        {
            Token = jwt.Mint("node-x", "test", TimeSpan.FromHours(1)), ConnectDataPlane = false, AnnouncedDataPlaneClientId = owner.ClientId,
        });
        Assert.Single(host.Host.Services.GetRequiredService<ReplicaRegistry>().Replicas("node-x"));
        await owner.ConnectAsync();

        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
        var ex = await Assert.ThrowsAsync<NoEligibleReplicaException>(
            () => orchestrator.RunAsync(Run("run-1", Fragment("frag-x", "node-x"))).WaitAsync(Wait));

        Assert.Contains("another subject", ex.Reasons["frag-x"]);
        Assert.Empty(claimant.Launches);
    }

    /// <summary>
    /// A replica's TransportR client was registered with the groups its node had then. The rights change, and nothing has
    /// registered it again: it is no replica to place a run on, until the groups it holds are the node's.
    /// </summary>
    [Fact]
    public async Task AReplicaWhoseDataPlaneHoldsOtherGroupsThanTheNodes_IsNeverChosen()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        await using var node = await FakeNode.StartAsync(host, "node-x", As(jwt, "node-x"));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var ex = await Assert.ThrowsAsync<NoEligibleReplicaException>(
            () => orchestrator.RunAsync(Run("run-1", Fragment("frag-x", "node-x", groups: ["audit"]))).WaitAsync(Wait));
        Assert.Contains("holds the groups [test]", ex.Reasons["frag-x"]);
        Assert.Empty(node.Launches);

        var run = orchestrator.RunAsync(Run("run-2", Fragment("frag-x", "node-x", groups: ["test"])));
        await LaunchedAsync(node, run);
        await node.ExitAsync(0);
        Assert.Equal(RunOutcome.Succeeded, (await run.WaitAsync(Wait)).Outcome);
    }

    private static BrickInfo Brick(string input) => TestBricks.Announced("src", BrickKind.Source, $$"""{"input":"{{input}}"}""", "source");

    /// <summary>
    /// A rolling update leaves two replicas of one node at two versions of a brick. The run is placed on the one that composes
    /// the version of the plan, whichever the tie would have gone to, and it is refused only when none does.
    /// </summary>
    [Fact]
    public async Task OfTwoReplicas_OnlyTheOneThatComposesThePlansVersionIsChosen()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        var units = new[] { new FragmentUnit("src", FragmentUnitKind.SourceSend, "src") };
        var spec = new FragmentSpec("frag-x", FragmentJob.VersionOf(units, FragmentJob.VersionsOver([Brick("sqlite:a")])), [], Units: units);

        // The changed replica has the lowest instance id: a tie-break alone would have chosen it.
        await using var changed = await FakeNode.StartAsync(host, "node-x", new FakeNodeOptions
        {
            Token = jwt.Mint("node-x", "test", TimeSpan.FromHours(1)), Role = NodeRole.Data, Bricks = [Brick("sqlite:b")],
            InstanceId = Guid.Parse("00000000-0000-0000-0000-000000000001"),
        });
        await using var unchanged = await FakeNode.StartAsync(host, "node-x", new FakeNodeOptions
        {
            Token = jwt.Mint("node-x", "test", TimeSpan.FromHours(1)), Role = NodeRole.Data, Bricks = [Brick("sqlite:a")],
            InstanceId = Guid.Parse("00000000-0000-0000-0000-000000000002"),
        });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var run = orchestrator.RunAsync(Run("run-1", new RunFragment(spec, "node-x", ["test"])));
        await LaunchedAsync(unchanged, run);
        await unchanged.ExitAsync(0);
        Assert.Equal(RunOutcome.Succeeded, (await run.WaitAsync(Wait)).Outcome);
        Assert.Empty(changed.Launches);

        // Only a replica that changed is left: the run is refused, naming the brick.
        await unchanged.LeaveAsync();
        var ex = await Assert.ThrowsAsync<NoEligibleReplicaException>(
            () => orchestrator.RunAsync(Run("run-2", new RunFragment(spec, "node-x", ["test"]))).WaitAsync(Wait));
        Assert.Contains("changed since the plan", ex.Reasons["frag-x"]);
    }

    /// <summary>The replica refuses the launch: nothing ran, and the reason, not the fragment, is the verdict's cause.</summary>
    [Fact]
    public async Task ARefusedLaunch_IsTheCause_AsAMessage_AndNoFragmentIsNamedUnresponsive()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });
        await using var a = await FakeNode.StartAsync(host, "a");
        await using var b = await FakeNode.StartAsync(host, "b");
        b.OnLaunch = _ => Task.FromResult(new LaunchResult(false, "a brick of b (src) changed since the plan was made: launch it again."));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var result = await orchestrator.RunAsync(Run("run-1", TestFragments.Edgeless("a"), TestFragments.Edgeless("b"))).WaitAsync(Wait);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Contains("changed since the plan was made", result.Cause);
        Assert.False(result.CauseIsUnresponsive);
        Assert.True(a.Cancelled.Task.IsCompleted);
        Assert.Equal(["a", "b"], result.Replicas!.Keys.Order());
    }

    /// <summary>
    /// Two runs on the two replicas of one node. One replica says it is leaving: the run placed on it ends naming its
    /// fragment, and the run on the other goes on.
    /// </summary>
    [Fact]
    public async Task TheLossOfAReplica_EndsOnlyTheRunsPlacedOnIt()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });
        // Two replicas of the node "n"; the second run goes to the replica the first run did not take.
        await using var r1 = await FakeNode.StartAsync(host, "n", new FakeNodeOptions { InstanceId = Guid.Parse("00000000-0000-0000-0000-000000000001") });
        await using var r2 = await FakeNode.StartAsync(host, "n", new FakeNodeOptions { InstanceId = Guid.Parse("00000000-0000-0000-0000-000000000002") });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var first = orchestrator.RunAsync(Run("run-1", TestFragments.Edgeless("x", "n")));
        await LaunchedAsync(r1, first);
        var second = orchestrator.RunAsync(Run("run-2", TestFragments.Edgeless("y", "n")));
        await LaunchedAsync(r2, second);

        await r1.LeaveAsync();
        var lost = await first.WaitAsync(Wait);
        Assert.Equal(RunOutcome.Failed, lost.Outcome);
        Assert.Equal("x", lost.Cause);
        Assert.True(lost.CauseIsUnresponsive);
        await Task.Delay(300);
        Assert.False(second.IsCompleted);
        Assert.False(r2.Cancelled.Task.IsCompleted);

        await r2.ExitAsync(0);
        Assert.Equal(RunOutcome.Succeeded, (await second.WaitAsync(Wait)).Outcome);
    }

    /// <summary>The data hub serves the data plane and nothing about a run: a call to a method that named a fragment or reported it is an error, not a no-op.</summary>
    [Theory]
    [InlineData("Register")]
    [InlineData("Unregister")]
    [InlineData("Ready")]
    [InlineData("Exited")]
    public async Task TheDataHubTakesNoMessageAboutARun(string method)
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });
        await using var client = host.CreateClient();
        await client.ConnectAsync();

        object?[] arguments = method switch
        {
            "Register" => ["frag-a", "v1"],
            "Ready" => ["run-1"],
            "Exited" => ["run-1", 1, "Local", null, new Dictionary<string, long>()],
            _ => [],
        };
        var ex = await Assert.ThrowsAsync<HubException>(() => client.ControlConnection.InvokeCoreAsync(method, typeof(object), arguments));

        Assert.Contains("does not exist", ex.Message);
    }
}
