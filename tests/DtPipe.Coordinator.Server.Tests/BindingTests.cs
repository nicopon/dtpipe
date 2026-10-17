using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;
using TransportR.FlowControl;
using TransportR.Services;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A pipeline's bindings name the node and the brick of each brick branch of its job, never the runner. They hold while every such
/// branch is bound to a node that exists and offers the brick its text designates there; otherwise the pipeline lists why, and a
/// node that is only offline does not block.
/// </summary>
public sealed class BindingTests : IDisposable
{
    private const string Source = """{"input":"sqlite:a","limit":10}""";
    private const string Job = "# P\ncustomers:\n  input: sqlite:a\n  limit: 10\nsink:\n  from: customers\n  output: csv:out.csv\n";

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "binding-" + Guid.NewGuid().ToString("N"));
    private readonly ReplicaRegistry _registry = new(new ReplicaRegistryOptions { DisconnectGracePeriod = TimeSpan.FromHours(1) });
    private readonly NodeInventory _inventory;
    private readonly AutoPlacer _placer;
    private readonly RightsStore _rights;

    public BindingTests()
    {
        _inventory = new NodeInventory(_registry);
        Directory.CreateDirectory(_dir);
        var seed = Path.Combine(_dir, "rights.json");
        File.WriteAllText(seed, """
            { "identities": [
              { "clientId": "node-a", "name": "node-a", "description": "", "groups": ["crm"] },
              { "clientId": "node-b", "name": "node-b", "description": "", "groups": ["crm"] },
              { "clientId": "runner-1", "name": "runner-1", "description": "", "groups": ["crm"] },
              { "clientId": "node-c", "name": "node-c", "description": "", "groups": ["crm"] },
              { "clientId": "node-off", "name": "node-off", "description": "", "groups": ["crm"], "enabled": false },
              { "clientId": "watch", "name": "watch", "description": "", "kind": "monitor" } ], "matrix": {} }
            """);
        _rights = new RightsStore(new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, "state"), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
        });
        _placer = new AutoPlacer(new BrickCatalog(_inventory), _inventory, null!, _rights);
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

    private static Dictionary<string, Binding> On(string node, string brick = "customers") => new() { ["customers"] = new Binding(node, brick) };

    private IReadOnlyList<BindingProblem> Check(Dictionary<string, Binding> bindings) => _placer.Check(Job, bindings);

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

        Assert.Equal(("customers", BindingCauses.Unassigned, true), (problem.Alias, problem.Cause, problem.Blocking));
        Assert.Contains("node-a", problem.Message);
        Assert.Contains("node-b", problem.Message);
    }

    [Fact]
    public void Two_Nodes_Offering_The_Same_Brick_Are_Told_Apart_By_The_Binding()
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

        Assert.Equal((BindingCauses.UnknownNode, true), (problem.Cause, problem.Blocking));
    }

    [Theory]
    [InlineData("strict")]
    [InlineData("tolerant")]
    public void A_Node_With_An_Identity_That_Has_Not_Announced_Is_Absent_Whatever_The_Mode(string mode)
    {
        var pin = mode == "strict" ? null : new BindingPin("v");
        var bindings = new Dictionary<string, Binding> { ["customers"] = new Binding("node-c", "customers", mode, pin) };

        var problem = Assert.Single(Check(bindings));

        Assert.Equal((BindingCauses.Offline, false), (problem.Cause, problem.Blocking));
    }

    [Theory]
    [InlineData("node-off")]
    [InlineData("watch")]
    public void A_Disabled_Identity_And_A_Monitors_Name_Are_No_Node(string name)
    {
        Announce("node-a", SourceBrick());

        var problem = Assert.Single(Check(On(name)));

        Assert.Equal((BindingCauses.UnknownNode, true), (problem.Cause, problem.Blocking));
    }

    [Fact]
    public void A_Node_That_Does_Not_Offer_The_Brick_Is_Refused_By_Name()
    {
        Announce("node-a", SourceBrick());
        Announce("node-b");

        var problem = Assert.Single(Check(On("node-b")));

        Assert.Equal((BindingCauses.NotOffered, true), (problem.Cause, problem.Blocking));
        Assert.Contains("node-a", problem.Message);
    }

    [Fact]
    public void A_Node_With_No_Connected_Replica_Is_Offline_And_Does_Not_Block()
    {
        Announce("node-a", SourceBrick());
        _registry.Disconnected("c-node-a");

        var problem = Assert.Single(Check(On("node-a")));

        Assert.Equal((BindingCauses.Offline, false), (problem.Cause, problem.Blocking));
    }

    [Fact]
    public void The_Runner_Is_Never_Named_And_A_Branch_The_Job_Lacks_Is_Refused()
    {
        Announce("node-a", SourceBrick());
        var runner = new NodeAnnouncement("runner-1", "", [], NodeRole.Runner, [], InstanceId: Guid.NewGuid());
        _registry.Announce("c-runner-1", "runner-1", runner, ["crm"], () => { });
        _inventory.Announced("runner-1", "runner-1", runner, ["crm"]);
        var bindings = On("node-a");
        bindings["sink"] = new Binding("runner-1", "sink");
        bindings["ghost"] = new Binding("node-a", "ghost");

        var problems = Check(bindings);

        Assert.Equal(
            [("sink", BindingCauses.NotABrick), ("ghost", BindingCauses.NoBranch)],
            problems.Select(p => (p.Alias, p.Cause)));
        Assert.All(problems, p => Assert.True(p.Blocking));
    }

    private PlanBuilder Planner() => new(_inventory, new NodeRequests(_inventory, null!), new BrickCatalog(_inventory),
        new PlanRegistry(new DefaultFlowControlService(new FlowControlOptions())), _rights);

    [Fact]
    public async Task A_Placed_Branch_Whose_Text_Is_No_Longer_A_Brick_Is_Never_Sent_To_The_Runner()
    {
        Announce("node-a", SourceBrick());
        var announcement = new NodeAnnouncement("runner-1", "", [], NodeRole.Runner, [], InstanceId: Guid.NewGuid());
        _registry.Announce("c-runner-1", "runner-1", announcement, ["crm"], () => { });
        _inventory.Announced("runner-1", "runner-1", announcement, ["crm"]);
        var placer = new AutoPlacer(new BrickCatalog(_inventory), _inventory, Planner(), _rights);

        var plan = await placer.PlanAsync("p", Job.Replace("sqlite:a", "sqlite:b"), On("node-a"), _inventory.Snapshot().Single(n => n.Name == "runner-1"), Ct);

        Assert.DoesNotContain(plan.Units, u => u.Id == "customers" && u.Node == "runner-1");
        Assert.False(plan.Deployable);
        var error = Assert.Single(plan.Errors, e => e.Contains("is bound to"));
        Assert.Contains("node-a", error);
        Assert.Contains("brick 'customers'", error);
    }

    private PipelineLibrary Library(Func<string, IReadOnlyDictionary<string, Binding>, IReadOnlyList<BindingProblem>>? problems = null) =>
        new(new CoordinatorServerOptions
        {
            StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret",
        }, problems, _placer.Resolve);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void A_Bound_Brick_Is_Checked_Against_The_Brick_The_Text_Designates_On_The_Node()
    {
        Announce("node-a", SourceBrick());

        var problem = Assert.Single(Check(On("node-a", "orders")));

        Assert.Equal((BindingCauses.BrickMismatch, true), (problem.Cause, problem.Blocking));
        Assert.Contains("customers", problem.Message);
        Assert.Contains("orders", problem.Message);
    }

    [Fact]
    public void A_Mode_That_Is_None_Of_The_Four_Is_Refused_By_Name()
    {
        Announce("node-a", SourceBrick());
        var bindings = new Dictionary<string, Binding> { ["customers"] = new Binding("node-a", "customers", "loose") };

        var problem = Assert.Single(Check(bindings));

        Assert.Equal((BindingCauses.UnsupportedMode, true), (problem.Cause, problem.Blocking));
        Assert.Contains("loose", problem.Message);
        Assert.Contains("tolerant", problem.Message);
    }

    [Theory]
    [InlineData("contract")]
    [InlineData("compatible")]
    [InlineData("tolerant")]
    public void A_Binding_In_A_Mode_Other_Than_Strict_Is_Unchecked_Until_A_Check_Pins_It(string mode)
    {
        Announce("node-a", SourceBrick());

        var unpinned = Assert.Single(Check(new() { ["customers"] = new Binding("node-a", "customers", mode) }));
        var pinned = Check(new() { ["customers"] = new Binding("node-a", "customers", mode, new BindingPin("v", "{}")) });

        Assert.Equal((BindingCauses.Unchecked, true), (unpinned.Cause, unpinned.Blocking));
        Assert.Empty(pinned);
    }

    [Fact]
    public void A_Pin_Without_The_Contract_Its_Mode_Compares_Is_Unchecked_But_A_Tolerant_One_Needs_None()
    {
        Announce("node-a", SourceBrick());

        Assert.Equal(BindingCauses.Unchecked, Assert.Single(Check(new() { ["customers"] = new Binding("node-a", "customers", "compatible", new BindingPin("v")) })).Cause);
        Assert.Empty(Check(new() { ["customers"] = new Binding("node-a", "customers", "tolerant", new BindingPin("v")) }));
    }

    [Fact]
    public void A_Binding_In_Another_Mode_Holds_Its_Brick_By_Id_Whatever_The_Branch_Text_Is_Now()
    {
        // The node offers the brick as its owner changed it; the job still holds the old text.
        Announce("node-a", new BrickInfo("customers", BrickKind.Source, "t", "", """{"input":"sqlite:other"}""", null, null));
        var tolerant = new Dictionary<string, Binding> { ["customers"] = new Binding("node-a", "customers", "tolerant", new BindingPin("v")) };
        var strict = new Dictionary<string, Binding> { ["customers"] = new Binding("node-a", "customers") };

        Assert.Empty(Check(tolerant));
        Assert.Equal("node-a/customers", _placer.BrickAliases(Job, tolerant)["customers"]);
        Assert.Equal(BindingCauses.NotABrick, Assert.Single(Check(strict)).Cause);
    }

    [Fact]
    public void A_Binding_In_Another_Mode_Names_The_Node_Or_The_Brick_That_Is_Missing()
    {
        Announce("node-a", SourceBrick());
        Binding Held(string node, string? brick) => new(node, brick, "tolerant", new BindingPin("v"));

        Assert.Equal(BindingCauses.UnknownNode, Assert.Single(Check(new() { ["customers"] = Held("node-z", "customers") })).Cause);
        Assert.Equal(BindingCauses.NotABrick, Assert.Single(Check(new() { ["customers"] = Held("node-a", null) })).Cause);
        Assert.Equal(BindingCauses.NotOffered, Assert.Single(Check(new() { ["customers"] = Held("node-a", "gone") })).Cause);
    }

    [Fact]
    public void A_Binding_That_Names_No_Brick_Gets_The_One_The_Text_Designates_On_Its_Node()
    {
        Announce("node-a", SourceBrick());
        Announce("node-b", SourceBrick());

        var resolved = _placer.Resolve(Job, new Dictionary<string, Binding> { ["customers"] = new Binding("node-b") });

        Assert.Equal(new Binding("node-b", "customers"), resolved["customers"]);
    }

    [Fact]
    public async Task A_Binding_Is_Kept_Beside_The_Job_And_Read_Back_With_Its_Brick_And_Mode()
    {
        Announce("node-a", SourceBrick());
        var library = Library();
        await library.SaveAsync("p", Job, "create", Ct);

        await library.SaveBindingsAsync("p", new Dictionary<string, Binding> { ["customers"] = new Binding("node-a") }, "bind", Ct);

        var document = (await library.GetAsync("p", Ct))!;
        Assert.Equal(new Binding("node-a", "customers", "strict"), document.Bindings["customers"]);
        Assert.Equal(Job, document.Yaml);
        var written = await File.ReadAllTextAsync(Path.Combine(_dir, "library", "pipelines", "p", "bindings.json"), Ct);
        Assert.Contains("\"node\": \"node-a\"", written);
        Assert.Contains("\"brick\": \"customers\"", written);
        Assert.Contains("\"mode\": \"strict\"", written);
    }

    [Fact]
    public async Task Bindings_Saved_After_The_Job_Do_Not_Change_The_Version_Of_The_Job()
    {
        var library = Library();
        await library.SaveAsync("p", Job, "create", Ct);
        await library.SaveBindingsAsync("p", On("node-a"), "bind", Ct);
        var jobCommit = (await library.GetAsync("p", Ct))!.Commit;

        await library.SaveBindingsAsync("p", On("node-b"), "move", Ct);

        var document = (await library.GetAsync("p", Ct))!;
        Assert.Equal("node-b", document.Bindings["customers"].Node);
        Assert.Equal(jobCommit, document.Commit);
    }

    [Fact]
    public async Task A_Job_Is_Read_At_A_Commit_Whatever_Was_Saved_Since()
    {
        var library = Library();
        await library.SaveAsync("p", Job, "create", Ct);
        var first = (await library.GetAsync("p", Ct))!.Commit!;
        await library.SaveAsync("p", Job + "# edited\n", "edit", Ct);

        Assert.Equal(Job, await library.JobAtAsync("p", first, Ct));
        Assert.Null(await library.JobAtAsync("p", "0000000", Ct));
        Assert.Null(await library.JobAtAsync("p", "not a commit", Ct));
    }

    [Fact]
    public async Task The_Problems_Of_The_Bindings_Are_Listed_With_The_Pipeline_And_The_Document()
    {
        var library = Library((_, bindings) => bindings.Select(b => new BindingProblem(b.Key, BindingCauses.UnknownNode, "gone", true)).ToList());
        await library.SaveAsync("p", Job, "create", Ct);
        await library.SaveBindingsAsync("p", On("node-gone"), "bind", Ct);

        var listed = Assert.Single(await library.ListAsync(Ct));

        Assert.Equal("customers", Assert.Single(listed.BindingProblems).Alias);
        Assert.Single((await library.GetAsync("p", Ct))!.BindingProblems);
    }

    [Fact]
    public async Task A_Seed_Brings_Its_Bindings()
    {
        var seeds = Path.Combine(_dir, "seed");
        Directory.CreateDirectory(seeds);
        await File.WriteAllTextAsync(Path.Combine(seeds, "p.yaml"), Job, Ct);
        await File.WriteAllTextAsync(Path.Combine(seeds, "p.bindings.json"), """{ "customers": { "node": "node-a", "brick": "customers", "mode": "strict" } }""", Ct);
        var library = new PipelineLibrary(new CoordinatorServerOptions
        {
            StateDir = _dir, LibrarySeed = seeds, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret",
        });

        Assert.Equal(new Binding("node-a", "customers", "strict"), (await library.GetAsync("p", Ct))!.Bindings["customers"]);
    }

    [Fact]
    public async Task A_Save_Sets_The_Bindings_It_Names_And_Keeps_The_Others_In_One_Commit()
    {
        Announce("node-a", SourceBrick());
        Announce("node-b", SourceBrick());
        var library = Library();
        await library.SaveAsync("p", Job, "create", Ct,
            bindings: new Dictionary<string, Binding> { ["customers"] = new Binding("node-a"), ["sink"] = new Binding("node-z", "sink") });
        var before = (await library.HistoryAsync("p", Ct)).Count;

        await library.SaveAsync("p", Job + "# edited\n", "edit", Ct, bindings: On("node-b"));

        var bindings = (await library.GetAsync("p", Ct))!.Bindings;
        Assert.Equal(("node-b", "node-z"), (bindings["customers"].Node, bindings["sink"].Node));
        Assert.Equal(before + 1, (await library.HistoryAsync("p", Ct)).Count);
    }

    [Fact]
    public async Task A_Save_With_No_Bindings_Leaves_Them_As_They_Are_And_Resolves_Their_Bricks_From_The_New_Text()
    {
        Announce("node-a", SourceBrick(), new BrickInfo("orders", BrickKind.Source, "t", "", """{"input":"sqlite:b","limit":10}""", null, null));
        var library = Library();
        await library.SaveAsync("p", Job, "create", Ct, bindings: On("node-a"));

        await library.SaveAsync("p", Job.Replace("sqlite:a", "sqlite:b"), "edit", Ct);

        Assert.Equal(new Binding("node-a", "orders", "strict"), (await library.GetAsync("p", Ct))!.Bindings["customers"]);
    }

    [Fact]
    public void The_Designers_Check_Takes_A_Brick_On_The_Node_The_Binding_Names()
    {
        Announce("node-a", SourceBrick());
        Announce("node-b", SourceBrick());
        var validator = new DesignValidator(new BrickCatalog(_inventory), null!);
        var branch = (YamlDotNet.RepresentationModel.YamlMappingNode)DtPipe.Jobs.JobYaml.FromJson(JsonNode.Parse(Source));

        Assert.Equal("node-b/customers", validator.BrickOf("customers", branch, On("node-b"))!.Key);
        Assert.Equal("node-a/customers", validator.BrickOf("customers", branch, On("node-a"))!.Key);
        Assert.Equal("node-a/customers", validator.BrickOf("customers", branch, new Dictionary<string, Binding>())!.Key);
        Assert.Equal("node-a/customers", validator.BrickOf("customers", branch, On("node-gone"))!.Key);
    }
}
