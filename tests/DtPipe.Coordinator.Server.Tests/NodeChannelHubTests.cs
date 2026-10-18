using System.Security.Claims;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Services;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

public sealed class NodeChannelHubTests : IDisposable
{
    private sealed class Caller(string connectionId, string subject) : HubCallerContext
    {
        public bool Aborted { get; private set; }
        public override string ConnectionId => connectionId;
        public override string? UserIdentifier => subject;
        public override ClaimsPrincipal? User { get; } = new(new ClaimsIdentity([new Claim("sub", subject), new Claim("team", "data")]));
        public override IDictionary<object, object?> Items { get; } = new Dictionary<object, object?>();
        public override IFeatureCollection Features { get; } = new FeatureCollection();
        public override CancellationToken ConnectionAborted => CancellationToken.None;
        public override void Abort() => Aborted = true;
    }

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "hub-" + Guid.NewGuid().ToString("N"));
    private readonly ReplicaRegistry _registry = new();
    private readonly NodeInventory _inventory;
    private readonly ReplicaProtocol _protocol;
    private readonly EnrollmentQueue _enrollment = new();
    private readonly EventBus _bus = new();
    private readonly RightsStore _rights;
    private readonly OrphanRunsTests.Abandonments _abandonments = new();
    private readonly OrphanRuns _orphans;
    private readonly CoordinatorServerOptions _options;

    public NodeChannelHubTests()
    {
        Directory.CreateDirectory(_dir);
        var seed = Path.Combine(_dir, "seed.json");
        File.WriteAllText(seed, """{ "identities": [ { "clientId": "known", "name": "node-known", "description": "", "groups": ["crm", "audit"] } ], "matrix": {} }""");
        _options = new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, "state"), Authority = "http://idp", Audience = "test",
            UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed, IdentityLabelClaims = ["team"],
        };
        _rights = new RightsStore(_options);
        _inventory = new NodeInventory(_registry);
        // A run the last coordinator left in flight on node-known.
        MemoryRunStore.Journal(_options).Record(new RunView(
            "run-3", "plan-1", RunState.Running, DateTimeOffset.UtcNow, null, null, null, null, "p", DateTimeOffset.UtcNow,
            new RunPlan("plan-1", null, [new RunPlanFragment("p@node-known", "node-known", "v1")], [])));
        _orphans = new OrphanRuns(MemoryRunStore.Journal(_options), _abandonments, NullLogger<OrphanRuns>.Instance);
        _protocol = new ReplicaProtocol(_registry, new NoRuns(), new InMemoryStateStore(), NullLogger<ReplicaProtocol>.Instance);
    }

    /// <summary>The orchestrator behind the protocol: no run is ever in flight here.</summary>
    private sealed class NoRuns : IRunOrchestrator
    {
        public Task<RunResult> RunAsync(RunSpec spec, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<ExitedAck> OnExitedAsync(ReplicaKey replica, FragmentExit exit) => Task.FromResult(ExitedAck.NotAdmitted);
    }

    public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }

    private (NodeChannelHub Hub, Caller Caller) Connect(string connectionId, string subject)
    {
        var caller = new Caller(connectionId, subject);
        return (new NodeChannelHub(_inventory, _protocol, _rights, _enrollment, _options, _bus, _orphans, NullLogger<NodeChannelHub>.Instance) { Context = caller }, caller);
    }

    private static NodeAnnouncement Proposing(string name) => new(name, "proposed description", [], NodeRole.Data, []);

    [Fact]
    public async Task A_Replica_Is_Asked_To_Abandon_A_Run_The_Restart_Left_Only_Once_Its_Data_Plane_Client_Is_Announced()
    {
        var (hub, _) = Connect("c1", "known");
        await hub.Announce(Proposing("node-known"));
        await Task.Delay(100);
        Assert.Empty(_abandonments.Asked);

        await hub.Announce(Proposing("node-known") with { InstanceId = Guid.NewGuid(), DataPlaneClientId = Guid.NewGuid() });
        for (var i = 0; i < 200 && _abandonments.Asked.IsEmpty; i++) await Task.Delay(25);

        var asked = Assert.Single(_abandonments.Asked);
        Assert.Equal(("known", "run-3", "p@node-known"), (asked.Replica.Node, asked.RunId, asked.Fragment));
    }

    [Fact]
    public async Task An_Unknown_Subject_Waits_And_Is_Never_A_Node()
    {
        var (hub, caller) = Connect("c1", "stranger");
        await hub.Announce(Proposing("node-known"));
        Assert.True(caller.Aborted);
        Assert.Empty(_inventory.Snapshot());
        var pending = Assert.Single(_enrollment.List());
        Assert.Equal("stranger", pending.Subject);
        Assert.Equal("node-known", pending.ProposedName);
        Assert.Equal("data", pending.Labels["team"]);
    }

    [Fact]
    public async Task A_Known_Host_Is_Filed_Under_Its_Identitys_Name_And_Groups_Whatever_It_Proposes()
    {
        var (hub, caller) = Connect("c1", "known");
        await hub.Announce(Proposing("some-other-name"));
        Assert.False(caller.Aborted);
        var node = Assert.Single(_inventory.Snapshot());
        Assert.Equal("node-known", node.Name);
        Assert.Equal(["audit", "crm"], node.Groups);
        Assert.Empty(_enrollment.List());
    }

    private IReadOnlyList<NodeLogLine> LogLines()
    {
        var (backlog, _, unsubscribe) = _bus.Subscribe();
        unsubscribe();
        return backlog.Where(e => e.Type == "log").Select(e => (NodeLogLine)e.Data).ToList();
    }

    [Fact]
    public async Task What_A_Host_Reports_Is_Filed_Under_The_Name_The_Rights_Gave_It()
    {
        var (hub, _) = Connect("c1", "known");
        await hub.Announce(Proposing("some-other-name"));
        await hub.ReportLog(new NodeLogLine("some-other-name", "p@node-known", "Information", "running"));
        var line = Assert.Single(LogLines());
        Assert.Equal("node-known", line.Node);
        Assert.Equal("running", line.Message);
    }

    [Fact]
    public async Task A_Report_On_A_Connection_That_Announced_Nothing_Is_Dropped()
    {
        var (hub, _) = Connect("c9", "known");
        await hub.ReportLog(new NodeLogLine("node-known", "p@node-known", "Information", "running"));
        Assert.Empty(LogLines());
        Assert.Empty(_inventory.Snapshot());
    }

    [Fact]
    public async Task A_Run_Report_On_A_Connection_That_Announced_Nothing_Is_Not_Heard()
    {
        var (hub, _) = Connect("c9", "known");
        var exit = new FragmentExit("run-1", "p@node-known", 1, "Local", "forged", new Dictionary<string, long>());
        Assert.Equal(ExitedAck.UnknownConnection, await hub.Exited(exit));
    }

    [Fact]
    public async Task A_Disabled_Identity_Is_Refused_And_Not_Queued()
    {
        _rights.UpdateIdentity("known", "node-known", "", ["crm"], enabled: false);
        var (hub, caller) = Connect("c1", "known");
        await hub.Announce(Proposing("node-known"));
        Assert.True(caller.Aborted);
        Assert.Empty(_inventory.Snapshot());
        Assert.Empty(_enrollment.List());
    }

    [Fact]
    public async Task A_Monitor_Is_Refused_At_The_Announcement_And_Is_Neither_Queued_Nor_A_Node()
    {
        _rights.AddIdentity(new Identity("watch", "supervisor", "", [], Kind: IdentityKinds.Monitor));
        var (hub, caller) = Connect("c1", "watch");

        await hub.Announce(Proposing("supervisor"));

        Assert.True(caller.Aborted);
        Assert.Empty(_inventory.Snapshot());
        Assert.Empty(_enrollment.List());
        Assert.Contains("a monitor, not a node", Assert.Single(LogLines()).Message);
    }

    [Fact]
    public async Task A_Launcher_Is_Refused_At_The_Announcement_And_Is_Neither_Queued_Nor_A_Node()
    {
        _rights.AddIdentity(new Identity("cron", "scheduler", "", [], Kind: IdentityKinds.Launcher, Pipelines: ["sales"]));
        var (hub, caller) = Connect("c1", "cron");

        await hub.Announce(Proposing("scheduler"));

        Assert.True(caller.Aborted);
        Assert.Empty(_inventory.Snapshot());
        Assert.Empty(_enrollment.List());
        Assert.Contains("a launcher, not a node", Assert.Single(LogLines()).Message);
    }

    [Fact]
    public async Task A_Node_Whose_Identity_Is_Removed_Leaves_The_Inventory_When_Its_Host_Goes()
    {
        var (hub, _) = Connect("c1", "known");
        await hub.Announce(Proposing("node-known"));
        _rights.RemoveIdentity("known");
        await hub.OnDisconnectedAsync(null);
        Assert.Empty(_inventory.Snapshot());
    }

    [Fact]
    public async Task A_Node_Whose_Identity_Stays_Remains_Listed_Offline()
    {
        var (hub, _) = Connect("c1", "known");
        await hub.Announce(Proposing("node-known"));
        await hub.OnDisconnectedAsync(null);
        var node = Assert.Single(_inventory.Snapshot());
        Assert.False(node.Online);
    }

    [Fact]
    public async Task A_Node_Stays_Online_While_Any_Of_Its_Replicas_Is_Connected()
    {
        var (first, _) = Connect("c1", "known");
        var (second, _) = Connect("c2", "known");
        await first.Announce(Proposing("node-known"));
        await second.Announce(Proposing("node-known"));

        await second.OnDisconnectedAsync(null);
        Assert.True(Assert.Single(_inventory.Snapshot()).Online);

        await first.OnDisconnectedAsync(null);
        Assert.False(Assert.Single(_inventory.Snapshot()).Online);
    }

    [Fact]
    public async Task What_Every_Replica_Logs_Is_Received_Under_The_Node()
    {
        var (first, _) = Connect("c1", "known");
        var (second, _) = Connect("c2", "known");
        await first.Announce(Proposing("node-known"));
        await second.Announce(Proposing("node-known"));

        await first.ReportLog(new NodeLogLine("node-known", "p@node-known", "Information", "from the first"));
        await second.ReportLog(new NodeLogLine("node-known", "q@node-known", "Information", "from the second"));

        Assert.Equal(["from the first", "from the second"], LogLines().Select(l => l.Message));
        Assert.All(LogLines(), l => Assert.Equal("node-known", l.Node));
    }

    [Fact]
    public async Task A_Replica_That_Leaves_Is_Gone_At_Once_And_The_Node_Goes_Offline_With_Its_Last()
    {
        var (first, _) = Connect("c1", "known");
        var (second, _) = Connect("c2", "known");
        await first.Announce(Replica(Guid.NewGuid()));
        await second.Announce(Replica(Guid.NewGuid()));

        await first.Leave();
        var node = Assert.Single(_inventory.Snapshot());
        Assert.True(node.Online);
        Assert.Single(node.Replicas);

        await second.Leave();
        Assert.False(Assert.Single(_inventory.Snapshot()).Online);
    }

    [Fact]
    public async Task A_Renamed_Identity_Is_The_Same_Node_Under_Its_New_Name()
    {
        var (hub, _) = Connect("c1", "known");
        await hub.Announce(Proposing("node-known"));
        _rights.UpdateIdentity("known", "node-renamed", "", ["crm", "audit"], enabled: true);
        var (again, _) = Connect("c2", "known");
        await again.Announce(Proposing("node-known"));

        var node = Assert.Single(_inventory.Snapshot());
        Assert.Equal("node-renamed", node.Name);
        Assert.Equal(2, node.Replicas.Count);
    }

    private static NodeAnnouncement Replica(Guid instance, string description = "proposed description", string? origin = null) =>
        new("node-known", description, [], NodeRole.Data, [], InstanceId: instance, Origin: origin);

    [Fact]
    public async Task A_Reconnection_Of_The_Same_Instance_Is_The_Same_Replica()
    {
        var instance = Guid.NewGuid();
        var (first, firstCaller) = Connect("c1", "known");
        var (again, _) = Connect("c3", "known");
        await first.Announce(Replica(instance));
        await again.Announce(Replica(instance));

        var node = Assert.Single(_inventory.Snapshot());
        Assert.Single(node.Replicas);
        Assert.Equal("c3", _registry.ConnectionOf(Assert.Single(_registry.Replicas("known")).Key));
        Assert.True(firstCaller.Aborted);
    }

    [Fact]
    public async Task The_Late_Close_Of_A_Replaced_Connection_Leaves_The_Replica_Online()
    {
        var instance = Guid.NewGuid();
        var (first, _) = Connect("c1", "known");
        var (again, _) = Connect("c3", "known");
        await first.Announce(Replica(instance));
        await again.Announce(Replica(instance));

        await first.OnDisconnectedAsync(null);

        var node = Assert.Single(_inventory.Snapshot());
        Assert.True(node.Online);
        Assert.Equal("c3", _registry.ConnectionOf(Assert.Single(_registry.Replicas("known")).Key));
    }

    [Fact]
    public async Task A_Node_Of_A_Removed_Identity_Is_Forgotten_With_Its_Last_Replica()
    {
        var (first, _) = Connect("c1", "known");
        var (second, _) = Connect("c2", "known");
        await first.Announce(Proposing("node-known"));
        await second.Announce(Proposing("node-known"));
        _rights.RemoveIdentity("known");

        await first.OnDisconnectedAsync(null);
        Assert.Single(_inventory.Snapshot());

        await second.OnDisconnectedAsync(null);
        Assert.Empty(_inventory.Snapshot());
    }

    /// <summary>
    /// What the node announces is read from its connected replica that arrived last, which a reconnection of the same host does not
    /// change; a read of the node goes to one of its replicas.
    /// </summary>
    [Fact]
    public async Task The_Node_Shows_Its_Most_Recent_Connected_Replica()
    {
        var (first, _) = Connect("c1", "known");
        var (second, _) = Connect("c2", "known");
        await first.Announce(Replica(Guid.NewGuid(), "older"));
        await Task.Delay(20);
        await second.Announce(Replica(Guid.NewGuid(), "newer"));

        Assert.Equal("newer", Assert.Single(_inventory.Snapshot()).Description);
        Assert.Equal(["c1", "c2"], _inventory.ReplicasOf("node-known").Select(r => r.ConnectionId).Order(StringComparer.Ordinal));

        await second.OnDisconnectedAsync(null);

        Assert.Equal("older", Assert.Single(_inventory.Snapshot()).Description);
        Assert.Equal(["c1"], _inventory.ReplicasOf("node-known").Select(r => r.ConnectionId));
    }

    [Fact]
    public async Task A_Rolling_Update_Shows_As_Replicas_That_Disagree_On_Their_Bricks()
    {
        BrickInfo Brick(string input) => new("src", BrickKind.Source, "source", "", $$"""{"input":"{{input}}"}""", null, null);
        var (first, _) = Connect("c1", "known");
        var (second, _) = Connect("c2", "known");
        await first.Announce(Replica(Guid.NewGuid()) with { Bricks = [Brick("sqlite:a")] });
        Assert.False(Assert.Single(_inventory.Snapshot()).ReplicasDisagree);

        await second.Announce(Replica(Guid.NewGuid()) with { Bricks = [Brick("sqlite:b")] });

        var node = Assert.Single(_inventory.Snapshot());
        Assert.True(node.ReplicasDisagree);
        Assert.Equal(2, node.Replicas.Select(r => r.Bricks).Distinct().Count());
    }

    [Fact]
    public async Task A_Replica_Origin_Is_Cut_Before_It_Is_Kept()
    {
        var (hub, _) = Connect("c1", "known");
        await hub.Announce(Replica(Guid.NewGuid(), origin: new string('x', 5000)));

        var replica = Assert.Single(Assert.Single(_inventory.Snapshot()).Replicas);
        Assert.InRange(replica.Origin!.Length, 1, 200);
    }

    [Fact]
    public async Task Disconnecting_An_Identity_Interrupts_Every_Replica()
    {
        var (first, firstCaller) = Connect("c1", "known");
        var (second, secondCaller) = Connect("c2", "known");
        await first.Announce(Proposing("node-known"));
        await second.Announce(Proposing("node-known"));

        Assert.Equal(2, _inventory.Disconnect("known"));

        Assert.True(firstCaller.Aborted);
        Assert.True(secondCaller.Aborted);
    }
}
