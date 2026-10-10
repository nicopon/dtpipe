using System.Security.Claims;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;
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
    private readonly NodeInventory _inventory = new();
    private readonly EnrollmentQueue _enrollment = new();
    private readonly EventBus _bus = new();
    private readonly RightsStore _rights;
    private readonly CoordinatorServerOptions _options;

    public NodeChannelHubTests()
    {
        Directory.CreateDirectory(_dir);
        var seed = Path.Combine(_dir, "seed.json");
        File.WriteAllText(seed, """{ "version": 2, "identities": [ { "clientId": "known", "name": "node-known", "description": "", "groups": ["crm", "audit"] } ], "matrix": {} }""");
        _options = new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, "state"), Authority = "http://idp", Audience = "test",
            UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed, IdentityLabelClaims = ["team"],
        };
        _rights = new RightsStore(_options);
    }

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private (NodeChannelHub Hub, Caller Caller) Connect(string connectionId, string subject)
    {
        var caller = new Caller(connectionId, subject);
        return (new NodeChannelHub(_inventory, _rights, _enrollment, _options, _bus, NullLogger<NodeChannelHub>.Instance) { Context = caller }, caller);
    }

    private static NodeAnnouncement Proposing(string name) => new(name, "proposed description", [], NodeRole.Data, []);

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

    [Fact]
    public async Task What_A_Host_Reports_Is_Filed_Under_The_Name_The_Rights_Gave_It()
    {
        var (hub, _) = Connect("c1", "known");
        await hub.Announce(Proposing("some-other-name"));
        await hub.ReportFragment(new FragmentStatus("some-other-name", "p@node-known", "main", "g1", FragmentState.Registered, "v", null, null, null));
        var node = Assert.Single(_inventory.Snapshot());
        Assert.Single(node.Fragments);
        Assert.Equal("node-known", node.Fragments[0].Node);
    }

    [Fact]
    public async Task A_Report_On_A_Connection_That_Announced_Nothing_Is_Dropped()
    {
        var (hub, _) = Connect("c9", "known");
        await hub.ReportFragment(new FragmentStatus("node-known", "p@node-known", "main", "g1", FragmentState.Registered, "v", null, null, null));
        Assert.Empty(_inventory.Snapshot());
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
    public async Task What_Every_Replica_Reports_Is_Received()
    {
        var (first, _) = Connect("c1", "known");
        var (second, _) = Connect("c2", "known");
        await first.Announce(Proposing("node-known"));
        await second.Announce(Proposing("node-known"));

        await first.ReportFragment(new FragmentStatus("node-known", "p@node-known", "main", "g1", FragmentState.Registered, "v", null, null, null));
        await second.ReportFragment(new FragmentStatus("node-known", "q@node-known", "main", "g1", FragmentState.Registered, "v", null, null, null));

        var node = Assert.Single(_inventory.Snapshot());
        Assert.Equal(["p@node-known", "q@node-known"], node.Fragments.Select(f => f.Fragment).Order(StringComparer.Ordinal));
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
        Assert.Equal("c3", _inventory.ConnectionOf("node-known"));
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
        Assert.Equal("c3", _inventory.ConnectionOf("node-known"));
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

    [Fact]
    public async Task The_Node_Shows_Its_Oldest_Connected_Replica_Which_Is_Also_The_One_Deployed_To()
    {
        var (first, _) = Connect("c1", "known");
        var (second, _) = Connect("c2", "known");
        await first.Announce(Replica(Guid.NewGuid(), "older"));
        await second.Announce(Replica(Guid.NewGuid(), "newer"));

        Assert.Equal("older", Assert.Single(_inventory.Snapshot()).Description);
        Assert.Equal("c1", _inventory.ConnectionOf("node-known"));
        Assert.Equal(["c1", "c2"], _inventory.ConnectionsOf("node-known").Order(StringComparer.Ordinal));

        await first.OnDisconnectedAsync(null);

        Assert.Equal("newer", Assert.Single(_inventory.Snapshot()).Description);
        Assert.Equal("c2", _inventory.ConnectionOf("node-known"));
        Assert.Equal(["c2"], _inventory.ConnectionsOf("node-known"));
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
