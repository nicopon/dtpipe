using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server.Access;
using DtPipe.Coordinator.Server.Deployments;
using DtPipe.Coordinator.Server.Library;
using DtPipe.Coordinator.Server.Nodes;
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
        Announce("d", "data", NodeRole.Data, [new DatasetInfo("LAB_DB", "")], [NodeAnnouncement.RawFragments]);
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
        return MemoryRightsStore.Rights(new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, $"state-{_seeds}"), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
        });
    }

    private PlanBuilder Planner(RightsStore rights) =>
        new(_inventory, new BrickCatalog(_inventory), new PlanRegistry(new DefaultFlowControlService(_matrix)), rights);

    private DistributionPlan Plan(RightsStore rights) =>
        Planner(rights).Build(
            new PlanRequest("p", Pair, new Dictionary<string, string> { ["a"] = "d", ["b"] = "r" }), PlanMode.Manual);

    private static void Allow(RightsStore rights, string from, string to) =>
        rights.AllowEdges([new GroupEdge(from, to)]);

    private DistributionPlan PlanJob(RightsStore rights, string yaml, IReadOnlyDictionary<string, string> placement) =>
        new PlanBuilder(_inventory, new BrickCatalog(_inventory), new PlanRegistry(new RightsFlowControl(rights)), rights)
            .Build(new PlanRequest("p", yaml, placement), PlanMode.Manual);

    [Fact]
    public async Task A_Plan_Carries_The_Groups_The_Rights_Give_A_Node_Now_Not_Those_Of_Its_Last_Announcement()
    {
        var rights = Rights(("d", ["audit"], true), ("r", ["run"], true));

        var plan = Plan(rights);

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

        var plan = Plan(rights);

        Assert.Equal(["'a' cannot be placed: Node 'd' is disabled: its identity may not connect, so no run is placed on it."], plan.Errors);
        Assert.Empty(plan.Fragments);
        Assert.False(plan.Deployable);
    }

    [Fact]
    public async Task A_Plan_Placed_On_A_Node_The_Rights_Do_Not_Know_Is_Refused_Saying_So()
    {
        var rights = Rights(("r", ["run"], true));

        var plan = Plan(rights);

        Assert.Equal(["'a' cannot be placed: Node 'd' has no identity in the rights: nothing says which groups it holds."], plan.Errors);
        Assert.Empty(plan.Fragments);
        Assert.False(plan.Deployable);
    }

    [Fact]
    public async Task A_Disabled_Runner_Refuses_The_Units_Placed_On_It_And_Not_The_Others()
    {
        var rights = Rights(("d", ["data"], true), ("r", ["run"], false));

        var plan = Plan(rights);

        Assert.Equal(["'b' cannot be placed: Node 'r' is disabled: its identity may not connect, so no run is placed on it."], plan.Errors);
        Assert.False(plan.Deployable);
    }

    [Fact]
    public async Task Transitive_Flow_Through_A_Runner_Requires_The_Source_Group_To_Reach_The_Sink_Group()
    {
        Announce("src", "crm", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);
        Announce("dst", "warehouse", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);

        var rights = Rights(
            ("src", ["crm"], true),
            ("r", ["run"], true),
            ("dst", ["warehouse"], true));

        Allow(rights, "crm", "run");
        Allow(rights, "run", "warehouse");

        const string job = "a:\n  input: generate:10\nb:\n  from: a\n  passthrough: true\nc:\n  from: b\n  output: csv:out.csv\n";
        var placement = new Dictionary<string, string> { ["a"] = "src", ["b"] = "r", ["c"] = "dst" };

        var plan = PlanJob(rights, job, placement);

        Assert.False(plan.Deployable);
        Assert.NotNull(plan.FlowRejection);
        Assert.Equal(2, plan.Edges.Count);

        var firstEdge = Assert.Single(plan.Edges, e => e.ProducerFragment == "p@src" && e.ConsumerFragment == "p@r");
        Assert.True(firstEdge.Allowed);
        Assert.Equal(["crm"], firstEdge.ProducerGroups);
        Assert.Equal(["run"], firstEdge.ConsumerGroups);

        var secondEdge = Assert.Single(plan.Edges, e => e.ProducerFragment == "p@r" && e.ConsumerFragment == "p@dst");
        Assert.False(secondEdge.Allowed);
        Assert.Equal(["crm", "run"], secondEdge.ProducerGroups);
        Assert.Equal(["warehouse"], secondEdge.ConsumerGroups);

        Allow(rights, "crm", "warehouse");

        var allowedPlan = PlanJob(rights, job, placement);

        Assert.True(allowedPlan.Deployable);
        Assert.Null(allowedPlan.FlowRejection);
        Assert.All(allowedPlan.Edges, e => Assert.True(e.Allowed));
    }

    [Fact]
    public async Task Join_On_A_Runner_Requires_Every_Upstream_Source_Group_To_Reach_The_Sink_Group()
    {
        Announce("n1", "crm", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);
        Announce("n2", "finance", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);
        Announce("dst", "warehouse", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);

        var rights = Rights(
            ("n1", ["crm"], true),
            ("n2", ["finance"], true),
            ("r", ["run"], true),
            ("dst", ["warehouse"], true));

        Allow(rights, "crm", "run");
        Allow(rights, "finance", "run");
        Allow(rights, "run", "warehouse");
        Allow(rights, "crm", "warehouse");

        const string job = "s1:\n  input: generate:10\ns2:\n  input: generate:10\nj:\n  from: s1\n  ref: s2\n  sql: \"SELECT * FROM s1, s2\"\nout:\n  from: j\n  output: csv:out.csv\n";
        var placement = new Dictionary<string, string> { ["s1"] = "n1", ["s2"] = "n2", ["j"] = "r", ["out"] = "dst" };

        var plan = PlanJob(rights, job, placement);

        Assert.False(plan.Deployable);
        Assert.NotNull(plan.FlowRejection);

        var outEdge = Assert.Single(plan.Edges, e => e.ProducerFragment == "p@r" && e.ConsumerFragment == "p@dst");
        Assert.False(outEdge.Allowed);
        Assert.Equal(["crm", "finance", "run"], outEdge.ProducerGroups);
        Assert.Equal(["warehouse"], outEdge.ConsumerGroups);

        Allow(rights, "finance", "warehouse");

        var allowedPlan = PlanJob(rights, job, placement);

        Assert.True(allowedPlan.Deployable);
        Assert.Null(allowedPlan.FlowRejection);
        Assert.All(allowedPlan.Edges, e => Assert.True(e.Allowed));
    }

    [Fact]
    public async Task Multi_Hop_Runner_Chain_Propagates_Source_Groups_Across_All_Hops()
    {
        Announce("src", "crm", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);
        Announce("r1", "run1", NodeRole.Runner, [], []);
        Announce("r2", "run2", NodeRole.Runner, [], []);
        Announce("dst", "warehouse", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);

        var rights = Rights(
            ("src", ["crm"], true),
            ("r1", ["run1"], true),
            ("r2", ["run2"], true),
            ("dst", ["warehouse"], true));

        Allow(rights, "crm", "run1");
        Allow(rights, "crm", "run2");
        Allow(rights, "run1", "run2");
        Allow(rights, "run1", "warehouse");
        Allow(rights, "run2", "warehouse");

        const string job = "a:\n  input: generate:10\nb:\n  from: a\n  passthrough: true\nc:\n  from: b\n  passthrough: true\nd:\n  from: c\n  output: csv:out.csv\n";
        var placement = new Dictionary<string, string> { ["a"] = "src", ["b"] = "r1", ["c"] = "r2", ["d"] = "dst" };

        var plan = PlanJob(rights, job, placement);

        Assert.False(plan.Deployable);
        Assert.NotNull(plan.FlowRejection);

        var hop2 = Assert.Single(plan.Edges, e => e.ProducerFragment == "p@r1" && e.ConsumerFragment == "p@r2");
        Assert.True(hop2.Allowed);
        Assert.Equal(["crm", "run1"], hop2.ProducerGroups);
        Assert.Equal(["run2"], hop2.ConsumerGroups);

        var hop3 = Assert.Single(plan.Edges, e => e.ProducerFragment == "p@r2" && e.ConsumerFragment == "p@dst");
        Assert.False(hop3.Allowed);
        Assert.Equal(["crm", "run1", "run2"], hop3.ProducerGroups);
        Assert.Equal(["warehouse"], hop3.ConsumerGroups);

        Allow(rights, "crm", "warehouse");

        var allowedPlan = PlanJob(rights, job, placement);

        Assert.True(allowedPlan.Deployable);
        Assert.Null(allowedPlan.FlowRejection);
        Assert.All(allowedPlan.Edges, e => Assert.True(e.Allowed));
    }

    [Fact]
    public async Task Wildcard_Runner_Rights_Cannot_Bypass_Transitive_Origin_Checks_Across_Single_Or_Multiple_Runners()
    {
        Announce("src", "crm", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);
        Announce("r1", "run1", NodeRole.Runner, [], []);
        Announce("r2", "run2", NodeRole.Runner, [], []);
        Announce("dst", "warehouse", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);

        var rights = Rights(
            ("src", ["crm"], true),
            ("r1", ["run1"], true),
            ("r2", ["run2"], true),
            ("dst", ["warehouse"], true));

        // Every group may send to the runners, and the runners may send to every group.
        Allow(rights, "*", "run1");
        Allow(rights, "run1", "*");
        Allow(rights, "*", "run2");
        Allow(rights, "run2", "*");

        const string job = "a:\n  input: generate:10\nb:\n  from: a\n  passthrough: true\nc:\n  from: b\n  passthrough: true\nd:\n  from: c\n  output: csv:out.csv\n";
        var placement = new Dictionary<string, string> { ["a"] = "src", ["b"] = "r1", ["c"] = "r2", ["d"] = "dst" };

        var plan = PlanJob(rights, job, placement);

        // Both runner hops pass via '* -> run1' and '* -> run2', but the edge into 'dst' is refused until 'crm -> warehouse' is allowed.
        Assert.False(plan.Deployable);
        Assert.NotNull(plan.FlowRejection);
        Assert.Contains("p@r2.c -> p@dst.c (crm+run1+run2 -> warehouse)", plan.FlowRejection);

        var hop1 = Assert.Single(plan.Edges, e => e.ProducerFragment == "p@src" && e.ConsumerFragment == "p@r1");
        Assert.True(hop1.Allowed);
        var hop2 = Assert.Single(plan.Edges, e => e.ProducerFragment == "p@r1" && e.ConsumerFragment == "p@r2");
        Assert.True(hop2.Allowed);
        var hop3 = Assert.Single(plan.Edges, e => e.ProducerFragment == "p@r2" && e.ConsumerFragment == "p@dst");
        Assert.False(hop3.Allowed);

        Allow(rights, "crm", "warehouse");

        var allowedPlan = PlanJob(rights, job, placement);

        Assert.True(allowedPlan.Deployable);
        Assert.Null(allowedPlan.FlowRejection);
        Assert.All(allowedPlan.Edges, e => Assert.True(e.Allowed));
    }

    [Fact]
    public async Task Wildcard_Runner_Rights_Cannot_Bypass_Missing_Group_In_Join_MultiGroup_Node_Or_FanOut()
    {
        Announce("n1", "crm", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);
        Announce("n2", "finance", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);
        Announce("dst1", "warehouse", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);
        Announce("dst2", "public", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);

        var rights = Rights(
            ("n1", ["crm", "pii"], true),
            ("n2", ["finance"], true),
            ("r", ["run"], true),
            ("dst1", ["warehouse", "audit"], true),
            ("dst2", ["public"], true));

        Allow(rights, "*", "run");
        Allow(rights, "run", "*");
        Allow(rights, "crm", "warehouse");
        Allow(rights, "crm", "audit");
        Allow(rights, "finance", "warehouse");
        Allow(rights, "finance", "audit");

        const string job = "s1:\n  input: generate:10\ns2:\n  input: generate:10\nj:\n  from: s1\n  ref: s2\n  sql: \"SELECT * FROM s1, s2\"\nout1:\n  from: j\n  output: csv:out1.csv\nout2:\n  from: j\n  output: csv:out2.csv\n";
        var placement = new Dictionary<string, string> { ["s1"] = "n1", ["s2"] = "n2", ["j"] = "r", ["out1"] = "dst1", ["out2"] = "dst2" };

        // Refused: 'pii' of n1 is not allowed to reach 'warehouse' or 'audit' on dst1, and none of the source groups may reach 'public' on dst2.
        var plan = PlanJob(rights, job, placement);
        Assert.False(plan.Deployable);
        Assert.False(Assert.Single(plan.Edges, e => e.ConsumerFragment == "p@dst1").Allowed);
        Assert.False(Assert.Single(plan.Edges, e => e.ConsumerFragment == "p@dst2").Allowed);

        // Allowing pii -> warehouse+audit makes dst1 allowed, while dst2 remains refused.
        Allow(rights, "pii", "warehouse");
        Allow(rights, "pii", "audit");

        var partialPlan = PlanJob(rights, job, placement);
        Assert.False(partialPlan.Deployable);
        Assert.True(Assert.Single(partialPlan.Edges, e => e.ConsumerFragment == "p@dst1").Allowed);
        Assert.False(Assert.Single(partialPlan.Edges, e => e.ConsumerFragment == "p@dst2").Allowed);
    }

    [Fact]
    public async Task Disconnected_Branches_On_The_Same_Runner_Cannot_Bypass_Transitive_Origin_Groups()
    {
        Announce("src", "crm", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);
        Announce("dst", "warehouse", NodeRole.Data, [], [NodeAnnouncement.RawFragments]);

        var rights = Rights(
            ("src", ["crm"], true),
            ("r", ["run"], true),
            ("dst", ["warehouse"], true));

        Allow(rights, "*", "run");
        Allow(rights, "run", "*");

        // Branch 'leak_in' receives from 'src' (crm) onto runner 'r'; branch 'leak_out' on the same runner 'r' declares no 'from: leak_in'
        // and feeds 'dst' (warehouse). Because both run in the same runner fragment process p@r, 'leak_out' inherits 'crm' as an origin.
        const string job = "s:\n  input: generate:10\nleak_in:\n  from: s\n  output: csv:tmp.csv\nleak_out:\n  input: csv:tmp.csv\nd:\n  from: leak_out\n  output: csv:out.csv\n";
        var placement = new Dictionary<string, string> { ["s"] = "src", ["leak_in"] = "r", ["leak_out"] = "r", ["d"] = "dst" };

        var plan = PlanJob(rights, job, placement);

        Assert.False(plan.Deployable);
        Assert.NotNull(plan.FlowRejection);
        var outEdge = Assert.Single(plan.Edges, e => e.ProducerFragment == "p@r" && e.ConsumerFragment == "p@dst");
        Assert.False(outEdge.Allowed);
        Assert.Equal(["crm", "run"], outEdge.ProducerGroups);
        Assert.Equal(["warehouse"], outEdge.ConsumerGroups);

        Allow(rights, "crm", "warehouse");

        var allowedPlan = PlanJob(rights, job, placement);
        Assert.True(allowedPlan.Deployable);
        Assert.Null(allowedPlan.FlowRejection);
    }
}
