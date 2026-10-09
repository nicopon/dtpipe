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
}
