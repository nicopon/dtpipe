using DtPipe.Coordinator.Contracts;
using TransportR.FlowControl;
using TransportR.Services;
using DtPipe.Tests.Shared;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// What each way of planning may do: only the Lab view's manual planning sends a data node a fragment
/// written as a job.
/// </summary>
public sealed class PlanModeTests : IDisposable
{
    private const string Pair = "a:\n  input: sqlite:${{LAB_DB}}\nb:\n  from: a\n  output: csv:out.csv\n";

    private readonly FlowControlOptions _matrix = new();
    private readonly ReplicaRegistry _registry = new();
    private readonly NodeInventory _inventory;
    private readonly PlanBuilder _planner;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "plan-mode-" + Guid.NewGuid().ToString("N"));

    public PlanModeTests()
    {
        Directory.CreateDirectory(_dir);
        var seed = Path.Combine(_dir, "seed.json");
        File.WriteAllText(seed, """
            { "identities": [
              { "clientId": "node-r", "name": "r", "description": "", "groups": ["run"] },
              { "clientId": "node-d", "name": "d", "description": "", "groups": ["data"] },
              { "clientId": "node-e", "name": "e", "description": "", "groups": ["store"] } ], "matrix": {} }
            """);
        var rights = MemoryRightsStore.Rights(new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, "state"), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
        });
        _inventory = new NodeInventory(_registry);
        _planner = new PlanBuilder(
            _inventory, new BrickCatalog(_inventory),
            new PlanRegistry(new DefaultFlowControlService(_matrix)), rights);
        _matrix.Groups["data"] = new GroupAccess { CanSendTo = ["run"] };

        Announce("r", "run", NodeRole.Runner, [], []);
    }

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private void Announce(string name, string group, NodeRole role, DatasetInfo[] datasets, string[] capabilities)
    {
        var announcement = new NodeAnnouncement(name, "", datasets, role, [], Capabilities: capabilities, InstanceId: Guid.NewGuid());
        _registry.Announce("connection-" + name, "node-" + name, announcement, [group], () => { });
        _inventory.Announced(name, "node-" + name, announcement, [group]);
    }

    /// <summary>A data node that hosts the database <c>LAB_DB</c>, with no brick: nothing it runs is a brick.</summary>
    private void DataNode(string name, bool acceptsRaw) =>
        Announce(name, "data", NodeRole.Data, [new DatasetInfo("LAB_DB", "")], acceptsRaw ? [NodeAnnouncement.RawFragments] : []);

    private DistributionPlan Plan(bool manual, params (string Unit, string Node)[] placement) =>
        _planner.Build(new PlanRequest("p", Pair, placement.ToDictionary(p => p.Unit, p => p.Node)), manual ? PlanMode.Manual : PlanMode.Library);

    [Fact]
    public void A_Fragment_That_Is_No_Brick_Goes_As_A_Job_When_The_Plan_Is_Manual_And_The_Node_Accepts_It()
    {
        DataNode("d", acceptsRaw: true);

        var plan = Plan(manual: true, ("a", "d"), ("b", "r"));

        Assert.Empty(plan.Errors);
        Assert.Equal(FragmentForm.Job, plan.Fragments.Single(f => f.Node == "d").Form);
        Assert.True(plan.Deployable);
    }

    [Fact]
    public void A_Plan_That_Is_Not_Manual_Refuses_A_Fragment_That_Is_No_Brick_Even_On_A_Node_That_Accepts_It()
    {
        DataNode("d", acceptsRaw: true);

        var plan = Plan(manual: false, ("a", "d"), ("b", "r"));

        Assert.Contains(plan.Errors, e => e.StartsWith("p@d:", StringComparison.Ordinal));
        Assert.DoesNotContain(plan.Fragments, f => f.Node == "d");
        Assert.False(plan.Deployable);
    }

    [Fact]
    public void A_Manual_Plan_Still_Needs_The_Node_To_Accept_A_Fragment_That_Is_No_Brick()
    {
        DataNode("d", acceptsRaw: false);

        var plan = Plan(manual: true, ("a", "d"), ("b", "r"));

        Assert.Contains(plan.Errors, e => e.StartsWith("p@d:", StringComparison.Ordinal));
        Assert.False(plan.Deployable);
    }

    [Fact]
    public void A_Brick_Reference_Reaches_No_Fragment_Written_As_A_Job_And_No_Runner()
    {
        DataNode("d", acceptsRaw: true);
        var job = "a:\n  input: brick:d/src\nb:\n  from: a\n  output: csv:out.csv\n";

        foreach (var (node, other) in new[] { ("d", "r"), ("r", "r") })
        {
            var plan = _planner.Build(
                new PlanRequest("p", job, new Dictionary<string, string> { ["a"] = node, ["b"] = other }), PlanMode.Manual);

            Assert.True(plan.Errors.Any(e => e.Contains("d/src", StringComparison.Ordinal)), $"{node}: " + string.Join(" / ", plan.Errors));
            Assert.False(plan.Deployable);
            Assert.DoesNotContain(plan.Fragments, f => f.Yaml.Contains("brick:", StringComparison.Ordinal) && f.Form == FragmentForm.Job);
        }
    }

    [Fact]
    public void A_Refused_Plan_Keeps_No_Edge_Whose_Fragment_Is_Not_In_It()
    {
        DataNode("d", acceptsRaw: true);
        var job = "a:\n  input: brick:d/src\nb:\n  from: a\n  output: csv:out.csv\n";

        var plan = _planner.Build(
            new PlanRequest("p", job, new Dictionary<string, string> { ["a"] = "d", ["b"] = "r" }), PlanMode.Manual);

        Assert.NotEmpty(plan.Errors);
        var named = plan.Fragments.Select(f => f.Name).ToHashSet(StringComparer.Ordinal);
        Assert.All(plan.Edges, e => Assert.True(
            named.Contains(e.ProducerFragment) && named.Contains(e.ConsumerFragment), $"{e.ProducerFragment} -> {e.ConsumerFragment}"));
    }

    [Fact]
    public void A_Sink_Reference_Is_Planned_As_A_Fragment_Of_Bricks_On_Its_Own_Node()
    {
        var source = TestBricks.Announced("src", BrickKind.Source, """{"input":"sqlite:a"}""");
        var sink = TestBricks.Announced("mirror", BrickKind.Sink, """{"output":"sqlite:b"}""");
        foreach (var (name, group, brick) in new[] { ("d", "data", source), ("e", "store", sink) })
        {
            var announcement = new NodeAnnouncement(name, "", [], NodeRole.Data, [brick], InstanceId: Guid.NewGuid());
            _registry.Announce("connection-" + name, "node-" + name, announcement, [group], () => { });
            _inventory.Announced(name, "node-" + name, announcement, [group]);
        }
        _matrix.Groups["data"] = new GroupAccess { CanSendTo = ["store"] };
        var job = "a:\n  input: brick:d/src\nb:\n  from: a\n  output: brick:e/mirror\n";

        var plan = _planner.Build(
            new PlanRequest("p", job, new Dictionary<string, string> { ["a"] = "d", ["b"] = "e" },
                new Dictionary<string, string> { ["a"] = "src", ["b"] = "mirror" }), PlanMode.Library);

        Assert.Empty(plan.Errors);
        Assert.True(plan.Deployable);
        Assert.All(plan.Fragments, f => Assert.Equal(FragmentForm.Bricks, f.Form));
        var consumer = plan.Fragments.Single(f => f.Node == "e");
        Assert.Contains(consumer.Composition!, u => u is { Kind: FragmentUnitKind.Sink, Brick: "mirror", From: "a" });
        Assert.Equal(FragmentJob.VersionOf(consumer.Composition!, (_, _) => sink.Version), consumer.Version);
    }

    [Fact]
    public void A_Fragment_Naming_A_Variable_That_Is_No_Identifier_Is_Refused_On_A_Node_That_Does_Not_Host_It()
    {
        DataNode("d", acceptsRaw: true);
        var job = "a:\n  input: sqlite:${{MY-VAR}}\nb:\n  from: a\n  output: csv:out.csv\n";

        var plan = _planner.Build(
            new PlanRequest("p", job, new Dictionary<string, string> { ["a"] = "d", ["b"] = "r" }), PlanMode.Manual);

        Assert.Contains(plan.Errors, e => e.Contains("MY-VAR", StringComparison.Ordinal));
        Assert.False(plan.Deployable);
    }

}
