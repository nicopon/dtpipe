using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;
using TransportR.FlowControl;
using TransportR.Services;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// What a plan takes of the rights: the groups a node holds now, and whether its identity may connect at all. The inventory
/// holds the groups of a node's last announcement, which a change of the rights or a disabled node leaves behind.
/// </summary>
public sealed class PlanRightsTests : IDisposable
{
    private sealed class NoHub : IHubContext<NodeChannelHub>
    {
        public IHubClients Clients => throw new NotSupportedException();
        public IGroupManager Groups => throw new NotSupportedException();
    }

    private const string Pair = "a:\n  input: sqlite:${{LAB_DB}}\nb:\n  from: a\n  output: csv:out.csv\n";

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "plan-rights-" + Guid.NewGuid().ToString("N"));
    private readonly FlowControlOptions _matrix = new();
    private readonly ReplicaRegistry _registry = new();
    private readonly NodeInventory _inventory;
    private int _seeds;

    public PlanRightsTests()
    {
        Directory.CreateDirectory(_dir);
        _inventory = new NodeInventory(_registry);
        foreach (var group in new[] { "data", "audit" }) _matrix.Groups[group] = new GroupAccess { CanSendTo = ["run"] };

        Announce("r", "run", NodeRole.Runner, [], []);
        Announce("d", "data", NodeRole.Data, [new DatasetInfo("LAB_DB", "sqlite", "")], [NodeAnnouncement.RawFragments]);
    }

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private void Announce(string name, string group, NodeRole role, DatasetInfo[] datasets, string[] capabilities)
    {
        var announcement = new NodeAnnouncement(name, "", datasets, role, [], Capabilities: capabilities, InstanceId: Guid.NewGuid());
        _registry.Announce("connection-" + name, "node-" + name, announcement, [group], () => { });
        _inventory.Announced(name, "node-" + name, announcement, [group]);
    }

    /// <summary>The rights, as a seed: one identity per node given, each with its groups and whether it is enabled.</summary>
    private RightsStore Rights(params (string Node, string[] Groups, bool Enabled)[] identities)
    {
        var seed = Path.Combine(_dir, $"seed-{_seeds++}.json");
        var rows = identities.Select(i => new { clientId = "node-" + i.Node, name = i.Node, description = "", groups = i.Groups, enabled = i.Enabled });
        File.WriteAllText(seed, JsonSerializer.Serialize(new { version = 2, identities = rows, matrix = new { } }, new JsonSerializerOptions(JsonSerializerDefaults.Web)));
        return new RightsStore(new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, $"state-{_seeds}"), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
        });
    }

    private PlanBuilder Planner(RightsStore rights) =>
        new(_inventory, new NodeRequests(_inventory, new NoHub()), new BrickCatalog(_inventory), new PlanRegistry(new DefaultFlowControlService(_matrix)), rights);

    private Task<DistributionPlan> PlanAsync(RightsStore rights) =>
        Planner(rights).BuildAsync(
            new PlanRequest("p", Pair, null, new Dictionary<string, string> { ["a"] = "d", ["b"] = "r" }), PlanMode.Manual, TestContext.Current.CancellationToken);

    [Fact]
    public async Task A_Plan_Carries_The_Groups_The_Rights_Give_A_Node_Now_Not_Those_Of_Its_Last_Announcement()
    {
        var rights = Rights(("d", ["audit"], true), ("r", ["run"], true));

        var plan = await PlanAsync(rights);

        Assert.Empty(plan.Errors);
        Assert.Equal(["audit"], plan.Fragments.Single(f => f.Node == "d").Groups);
        Assert.Equal(["run"], plan.Fragments.Single(f => f.Node == "r").Groups);
        Assert.Equal(["audit"], Assert.Single(plan.Edges).ProducerGroups);
        Assert.Null(plan.FlowRejection);
        Assert.True(plan.Deployable);
    }

    [Fact]
    public async Task A_Plan_Placed_On_A_Disabled_Node_Is_Refused_Saying_So()
    {
        var rights = Rights(("d", ["data"], false), ("r", ["run"], true));

        var plan = await PlanAsync(rights);

        Assert.Equal(["'a' cannot be placed: Node 'd' is disabled: its identity may not connect, so no run is placed on it."], plan.Errors);
        Assert.Empty(plan.Fragments);
        Assert.False(plan.Deployable);
    }

    [Fact]
    public async Task A_Plan_Placed_On_A_Node_The_Rights_Do_Not_Know_Is_Refused_Saying_So()
    {
        var rights = Rights(("r", ["run"], true));

        var plan = await PlanAsync(rights);

        Assert.Equal(["'a' cannot be placed: Node 'd' has no identity in the rights: nothing says which groups it holds."], plan.Errors);
        Assert.Empty(plan.Fragments);
        Assert.False(plan.Deployable);
    }

    [Fact]
    public async Task A_Disabled_Runner_Refuses_The_Units_Placed_On_It_And_Not_The_Others()
    {
        var rights = Rights(("d", ["data"], true), ("r", ["run"], false));

        var plan = await PlanAsync(rights);

        Assert.Equal(["'b' cannot be placed: Node 'r' is disabled: its identity may not connect, so no run is placed on it."], plan.Errors);
        Assert.False(plan.Deployable);
    }
}
