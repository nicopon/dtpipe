using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A pipeline's placement names the node of each brick branch of its job, never the runner. It holds while every such branch is
/// placed on a node that exists and offers the brick; otherwise the pipeline lists why, and a node that is only offline does not block.
/// </summary>
public sealed class PlacementTests : IDisposable
{
    private const string Source = """{"input":"sqlite:a","limit":10}""";
    private const string Job = "# P\ncustomers:\n  input: sqlite:a\n  limit: 10\nsink:\n  from: customers\n  output: csv:out.csv\n";

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "placement-" + Guid.NewGuid().ToString("N"));
    private readonly ReplicaRegistry _registry = new(new ReplicaRegistryOptions { DisconnectGracePeriod = TimeSpan.FromHours(1) });
    private readonly NodeInventory _inventory;
    private readonly AutoPlacer _placer;

    public PlacementTests()
    {
        _inventory = new NodeInventory(_registry);
        _placer = new AutoPlacer(new BrickCatalog(_inventory), _inventory, null!);
    }

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    private void Announce(string name, params BrickInfo[] bricks)
    {
        var announcement = new NodeAnnouncement(name, "", [], NodeRole.Data, bricks, InstanceId: Guid.NewGuid());
        _registry.Announce("c-" + name, name, announcement, ["crm"], () => { });
        _inventory.Announced(name, name, announcement, ["crm"]);
    }

    private static BrickInfo SourceBrick() => new("customers", BrickKind.Source, "t", "", Source, null, null);

    private static Dictionary<string, string> On(string node) => new() { ["customers"] = node };

    private IReadOnlyList<PlacementProblem> Check(Dictionary<string, string> placement) => _placer.Check(Job, placement);

    [Fact]
    public void A_Brick_Placed_On_The_Node_That_Offers_It_Has_No_Problem()
    {
        Announce("node-a", SourceBrick());

        Assert.Empty(Check(On("node-a")));
    }

    [Fact]
    public void A_Brick_Branch_With_No_Node_Is_Unassigned_And_Names_The_Nodes_That_Offer_It()
    {
        Announce("node-a", SourceBrick());
        Announce("node-b", SourceBrick());

        var problem = Assert.Single(Check([]));

        Assert.Equal(("customers", PlacementCauses.Unassigned, true), (problem.Alias, problem.Cause, problem.Blocking));
        Assert.Contains("node-a", problem.Message);
        Assert.Contains("node-b", problem.Message);
    }

    [Fact]
    public void Two_Nodes_Offering_The_Same_Brick_Are_Told_Apart_By_The_Placement()
    {
        Announce("node-a", SourceBrick());
        Announce("node-b", SourceBrick());

        Assert.Empty(Check(On("node-b")));
        Assert.Equal("node-b/customers", _placer.BrickAliases(Job, On("node-b"))["customers"]);
        Assert.Equal("node-a/customers", _placer.BrickAliases(Job, On("node-a"))["customers"]);
    }

    [Fact]
    public void A_Node_With_No_Identity_Is_Unknown()
    {
        Announce("node-a", SourceBrick());

        var problem = Assert.Single(Check(On("node-gone")));

        Assert.Equal((PlacementCauses.UnknownNode, true), (problem.Cause, problem.Blocking));
    }

    [Fact]
    public void A_Node_That_Does_Not_Offer_The_Brick_Is_Refused_By_Name()
    {
        Announce("node-a", SourceBrick());
        Announce("node-b");

        var problem = Assert.Single(Check(On("node-b")));

        Assert.Equal((PlacementCauses.NotOffered, true), (problem.Cause, problem.Blocking));
        Assert.Contains("node-a", problem.Message);
    }

    [Fact]
    public void A_Node_With_No_Connected_Replica_Is_Offline_And_Does_Not_Block()
    {
        Announce("node-a", SourceBrick());
        _registry.Disconnected("c-node-a");

        var problem = Assert.Single(Check(On("node-a")));

        Assert.Equal((PlacementCauses.Offline, false), (problem.Cause, problem.Blocking));
    }

    [Fact]
    public void The_Runner_Is_Never_Named_And_A_Branch_The_Job_Lacks_Is_Refused()
    {
        Announce("node-a", SourceBrick());
        var placement = On("node-a");
        placement["sink"] = "runner";
        placement["ghost"] = "node-a";

        var problems = Check(placement);

        Assert.Equal(
            [("sink", PlacementCauses.NotABrick), ("ghost", PlacementCauses.NoBranch)],
            problems.Select(p => (p.Alias, p.Cause)));
        Assert.All(problems, p => Assert.True(p.Blocking));
    }

    private PipelineLibrary Library(Func<string, IReadOnlyDictionary<string, string>, IReadOnlyList<PlacementProblem>>? problems = null) =>
        new(new CoordinatorServerOptions
        {
            StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret",
        }, _ => ["crm"], problems);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static DistributionPlan APlan() => new("plan-1", "p", [], [], [], [], [], [], null);

    [Fact]
    public async Task A_Placement_Is_Kept_Beside_The_Job_And_Read_Back()
    {
        var library = Library();
        await library.SaveAsync("p", Job, "create", Ct);

        await library.SavePlacementAsync("p", On("node-a"), "place", Ct);

        var document = (await library.GetAsync("p", Ct))!;
        Assert.Equal("node-a", document.Placement["customers"]);
        Assert.Equal(Job, document.Yaml);
        Assert.Contains("\"customers\": \"node-a\"", await File.ReadAllTextAsync(Path.Combine(_dir, "library", "pipelines", "p", "placement.json"), Ct));
    }

    [Fact]
    public async Task A_Placement_Saved_After_The_Plan_Makes_The_Plan_Stale_Without_Changing_The_Version_Of_The_Job()
    {
        var library = Library();
        await library.SaveAsync("p", Job, "create", Ct);
        await library.SavePlacementAsync("p", On("node-a"), "place", Ct);
        await library.SavePlanAsync("p", APlan(), "plan", Ct);
        var jobCommit = (await library.GetAsync("p", Ct))!.Commit;
        Assert.True((await library.GetAsync("p", Ct))!.PlanCurrent);

        var saved = await library.SavePlacementAsync("p", On("node-b"), "move", Ct);

        var document = (await library.GetAsync("p", Ct))!;
        Assert.True(saved.PlanStale);
        Assert.Equal((false, PlanStaleCauses.Placement), (document.PlanCurrent, document.PlanStaleCause));
        Assert.Equal(jobCommit, document.Commit);
    }

    [Fact]
    public async Task The_Problems_Of_A_Placement_Are_Listed_With_The_Pipeline_And_The_Document()
    {
        var library = Library((_, placement) => placement.Select(p => new PlacementProblem(p.Key, PlacementCauses.UnknownNode, "gone", true)).ToList());
        await library.SaveAsync("p", Job, "create", Ct);
        await library.SavePlacementAsync("p", On("node-gone"), "place", Ct);

        var listed = Assert.Single(await library.ListAsync(Ct));

        Assert.Equal("customers", Assert.Single(listed.PlacementProblems).Alias);
        Assert.Single((await library.GetAsync("p", Ct))!.PlacementProblems);
    }

    [Fact]
    public async Task A_Seed_Brings_Its_Placement()
    {
        var seeds = Path.Combine(_dir, "seed");
        Directory.CreateDirectory(seeds);
        await File.WriteAllTextAsync(Path.Combine(seeds, "p.yaml"), Job, Ct);
        await File.WriteAllTextAsync(Path.Combine(seeds, "p.placement.json"), """{ "customers": "node-a" }""", Ct);
        var library = new PipelineLibrary(new CoordinatorServerOptions
        {
            StateDir = _dir, LibrarySeed = seeds, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret",
        }, _ => ["crm"]);

        Assert.Equal("node-a", (await library.GetAsync("p", Ct))!.Placement["customers"]);
    }

    [Fact]
    public async Task A_Save_Sets_The_Placement_It_Names_And_Keeps_The_Others_In_One_Commit()
    {
        var library = Library();
        await library.SaveAsync("p", Job, "create", Ct, placement: new Dictionary<string, string> { ["customers"] = "node-a", ["sink"] = "node-z" });
        var before = (await library.HistoryAsync("p", Ct)).Count;

        await library.SaveAsync("p", Job + "# edited\n", "edit", Ct, placement: On("node-b"));

        var placement = (await library.GetAsync("p", Ct))!.Placement;
        Assert.Equal(("node-b", "node-z"), (placement["customers"], placement["sink"]));
        Assert.Equal(before + 1, (await library.HistoryAsync("p", Ct)).Count);
    }

    [Fact]
    public async Task A_Save_With_No_Placement_Leaves_It_As_It_Is()
    {
        var library = Library();
        await library.SaveAsync("p", Job, "create", Ct, placement: On("node-a"));

        await library.SaveAsync("p", Job + "# edited\n", "edit", Ct);

        Assert.Equal("node-a", (await library.GetAsync("p", Ct))!.Placement["customers"]);
    }

    [Fact]
    public void The_Designers_Check_Takes_A_Brick_On_The_Node_The_Placement_Names()
    {
        Announce("node-a", SourceBrick());
        Announce("node-b", SourceBrick());
        var validator = new DesignValidator(new BrickCatalog(_inventory), null!);
        var branch = (YamlDotNet.RepresentationModel.YamlMappingNode)DtPipe.Jobs.JobYaml.FromJson(JsonNode.Parse(Source));

        Assert.Equal("node-b/customers", validator.BrickOf("customers", branch, On("node-b"))!.Key);
        Assert.Equal("node-a/customers", validator.BrickOf("customers", branch, On("node-a"))!.Key);
        Assert.Equal("node-a/customers", validator.BrickOf("customers", branch, new Dictionary<string, string>())!.Key);
        Assert.Equal("node-a/customers", validator.BrickOf("customers", branch, On("node-gone"))!.Key);
    }
}
