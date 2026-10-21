using System.Collections.Concurrent;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Services;
using Xunit;
using DtPipe.Tests.Shared;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A deployment is the coordinator's: it asks the replicas of each node whether they would run the fragments, and is
/// ready when every fragment has a replica that said yes and that a run could be placed on. Nothing is left on a replica.
/// </summary>
public sealed class DeploymentManagerTests : IDisposable
{
    private sealed class Replicas : INodeLink
    {
        public ConcurrentDictionary<ReplicaKey, FragmentCheck> Answers { get; } = new();
        public ConcurrentQueue<(ReplicaKey Replica, string Fragment)> Checked { get; } = new();

        public Task<FragmentCheck> CheckAsync(ReplicaKey replica, FragmentSpec spec, CancellationToken ct)
        {
            Checked.Enqueue((replica, spec.Fragment));
            return Task.FromResult(Answers.GetValueOrDefault(replica, new FragmentCheck(true)));
        }

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

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "deploy-" + Guid.NewGuid().ToString("N"));
    private readonly RightsStore _rights;
    private readonly ReplicaRegistry _registry = new();
    private readonly InMemoryStateStore _state = new();
    private readonly NodeInventory _inventory;
    private readonly Replicas _link = new();
    private readonly RunJournal _journal;
    private readonly DeploymentManager _deployments;

    public DeploymentManagerTests()
    {
        Directory.CreateDirectory(_dir);
        var seed = Path.Combine(_dir, "seed.json");
        File.WriteAllText(seed, """{ "identities": [ { "clientId": "node-n", "name": "n", "description": "", "groups": ["crm"] } ], "matrix": {} }""");
        var options = new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, "state"), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
        };
        _rights = MemoryRightsStore.Rights(options);
        _inventory = new NodeInventory(_registry);
        _journal = MemoryRunStore.Journal(options);
        _deployments = TestDeployments.Create(
            _inventory, _link, new NoRuns(), new ReplicaAdmission(_registry, _state), new RightsFlowControl(_rights), _rights, new EventBus(),
            _journal, options);
    }

    public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

    private static BrickInfo Brick(string input) => TestBricks.WithText("src", BrickKind.Source, $$"""{"input":"{{input}}"}""", "source");

    /// <summary>A replica of the node "n", its TransportR client registered under the node's subject with the node's groups.</summary>
    private async Task<ReplicaKey> ReplicaAsync(int instance, params BrickInfo[] bricks)
    {
        var client = Guid.NewGuid();
        await _state.SaveClientAsync(new ClientRegistration(client, "data-" + instance, ["crm"], null, null, DateTime.UtcNow, Subject: "node-n"));
        var announcement = new NodeAnnouncement("n", "", [], NodeRole.Data, bricks, InstanceId: Guid.Parse($"00000000-0000-0000-0000-{instance:000000000000}"), DataPlaneClientId: client);
        var key = _registry.Announce("c" + instance, "node-n", announcement, ["crm"], () => { });
        _inventory.Announced("n", "node-n", announcement, ["crm"]);
        return key;
    }

    private static readonly FragmentUnit[] Units = [new("src", FragmentUnitKind.SourceSend, "src")];

    private static DistributionPlan Plan()
    {
        var fragment = new PlannedFragment(
            "p@n", "n", ["crm"], "", FragmentJob.VersionOf(Units, FragmentJob.VersionsOver([Brick("sqlite:a")])), [], ["src"], FragmentForm.Bricks, Composition: Units);
        return new DistributionPlan("plan-1", "p", [], [], [fragment], [], [], [], null);
    }

    /// <summary>
    /// A rolling update leaves one replica of the node at the old brick. Its refusal is not the deployment's: the other
    /// replica says yes and can take a run, so the deployment is ready, and it counts the one replica a run could use.
    /// </summary>
    [Fact]
    public async Task A_Replica_That_Refuses_Does_Not_Fail_The_Deployment_While_Another_Can_Run_It()
    {
        var old = await ReplicaAsync(1, Brick("sqlite:b"));
        var updated = await ReplicaAsync(2, Brick("sqlite:a"));
        _link.Answers[old] = new FragmentCheck(false, "a brick of p@n (src) changed since the plan was made: launch it again.");

        await _deployments.DeployAsync(Plan(), "test", CancellationToken.None);

        var deployment = Assert.Single(await _deployments.DeploymentsAsync());
        Assert.Equal(DeploymentState.Ready, deployment.State);
        Assert.Equal(1, Assert.Single(deployment.Fragments).Replicas);
        Assert.Equal(2, _link.Checked.Count);
        Assert.Contains(_link.Checked, c => c.Replica == updated);
    }

    [Fact]
    public async Task A_Deployment_Every_Replica_Refuses_Is_Refused_At_Once_Naming_The_Node_And_Is_Not_Kept()
    {
        var only = await ReplicaAsync(1, Brick("sqlite:a"));
        _link.Answers[only] = new FragmentCheck(false, "'copy' is not a read or a write of this node.");

        var ex = await Assert.ThrowsAsync<ConflictException>(() => _deployments.DeployAsync(Plan(), "test", CancellationToken.None));

        Assert.Contains("refused by n", ex.Message);
        Assert.Contains("'copy'", ex.Message);
        Assert.Empty(await _deployments.DeploymentsAsync());
    }

    /// <summary>
    /// A deployment that was ready stays in place while its node's identity is disabled; a run of it is refused at its
    /// start, naming the reason, and no replica is counted as able to run the fragment.
    /// </summary>
    [Fact]
    public async Task A_Run_On_A_Node_Whose_Identity_Is_Disabled_Is_Refused_And_Says_Why()
    {
        await ReplicaAsync(1, Brick("sqlite:a"));
        await _deployments.DeployAsync(Plan(), "test", CancellationToken.None);
        _rights.UpdateIdentity("node-n", "n", "", ["crm"], false);

        var queued = _deployments.Enqueue("p");

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        RunView? run;
        while ((run = _journal.List().FirstOrDefault(r => r.RunId == queued.RunId)) is null)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("the run was never finished");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
        Assert.Equal(RunState.Refused, run.State);
        Assert.Contains("disabled", run.Refusal);
        Assert.Equal(0, Assert.Single((await _deployments.DeploymentsAsync()).Single().Fragments).Replicas);
    }

    /// <summary>An identity that is removed is no identity: the run is refused as for a disabled one, and no replica counts.</summary>
    [Fact]
    public async Task A_Run_On_A_Node_Whose_Identity_Is_Removed_Is_Refused_And_Says_Why()
    {
        await ReplicaAsync(1, Brick("sqlite:a"));
        await _deployments.DeployAsync(Plan(), "test", CancellationToken.None);
        _rights.RemoveIdentity("node-n");

        var queued = _deployments.Enqueue("p");

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        RunView? run;
        while ((run = _journal.List().FirstOrDefault(r => r.RunId == queued.RunId)) is null)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("the run was never finished");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
        Assert.Equal(RunState.Refused, run.State);
        Assert.Contains("Node 'n' has no identity in the rights", run.Refusal);
        Assert.Equal(0, Assert.Single((await _deployments.DeploymentsAsync()).Single().Fragments).Replicas);
    }

    [Fact]
    public async Task A_Node_With_No_Replica_Online_Is_Offline()
    {
        var only = await ReplicaAsync(1, Brick("sqlite:a"));
        _registry.Disconnected("c1");

        var ex = await Assert.ThrowsAsync<ConflictException>(() => _deployments.DeployAsync(Plan(), "test", CancellationToken.None));

        Assert.Equal("Offline: n.", ex.Message);
        Assert.Empty(_link.Checked);
        _ = only;
    }

    [Fact]
    public async Task A_Replica_That_Arrives_After_The_Deployment_Is_Eligible_Without_Being_Asked()
    {
        await ReplicaAsync(1, Brick("sqlite:a"));
        await _deployments.DeployAsync(Plan(), "test", CancellationToken.None);

        await ReplicaAsync(2, Brick("sqlite:a"));

        Assert.Equal(2, Assert.Single(Assert.Single(await _deployments.DeploymentsAsync()).Fragments).Replicas);
        Assert.Single(_link.Checked);
    }
}
