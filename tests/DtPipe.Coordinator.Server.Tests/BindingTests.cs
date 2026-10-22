using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;
using TransportR.FlowControl;
using TransportR.Services;
using Xunit;
using DtPipe.Tests.Shared;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A pipeline's bindings hold the mode and the pin of each branch that references a brick, never the runner: the reference names the
/// node and the brick. They hold while every such branch names a node that exists and offers the brick; otherwise the pipeline
/// lists why, and a node that is only offline does not block.
/// </summary>
public sealed class BindingTests : IDisposable
{
    private const string Source = """{"input":"sqlite:a","limit":10}""";
    private static readonly string Job = JobOn("node-a");

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
        _rights = MemoryRightsStore.Rights(new CoordinatorServerOptions
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

    private static string JobOn(string node, string brick = "customers") =>
        $"# P\ncustomers:\n  input: brick:{node}/{brick}\nsink:\n  from: customers\n  output: csv:out.csv\n";

    private static BrickInfo SourceBrick() => TestBricks.Announced("customers", BrickKind.Source, Source);

    private static readonly string Version = SourceBrick().Version;

    /// <summary>The binding of the referenced alias: a reference names its node and its brick, so it holds the mode and the pin alone.</summary>
    private static Dictionary<string, Binding> Held(string? mode = null, BindingPin? pin = null) => new() { ["customers"] = new Binding("", null, mode, pin ?? new BindingPin(Version, mode is "contract" or "compatible" ? "{}" : null)) };

    private IReadOnlyList<BindingProblem> Check(string job, Dictionary<string, Binding>? bindings = null) => _placer.Check(job, bindings ?? Held());

    [Fact]
    public void A_Brick_Referenced_On_A_Node_That_Offers_It_Has_No_Problem()
    {
        Announce("node-a", SourceBrick());

        Assert.Empty(Check(Job));
    }

    [Fact]
    public void A_Reference_Names_Its_Node_Among_Those_That_Offer_The_Same_Brick()
    {
        Announce("node-a", SourceBrick());
        Announce("node-b", SourceBrick());

        Assert.Empty(Check(JobOn("node-b")));
        Assert.Equal("node-b/customers", _placer.BrickAliases(JobOn("node-b"))["customers"]);
        Assert.Equal("node-a/customers", _placer.BrickAliases(JobOn("node-a"))["customers"]);
    }

    [Fact]
    public void A_Node_With_No_Identity_Is_Unknown()
    {
        Announce("node-a", SourceBrick());

        var problem = Assert.Single(Check(JobOn("node-gone")));

        Assert.Equal((BindingCauses.UnknownNode, true), (problem.Cause, problem.Blocking));
    }

    [Theory]
    [InlineData("strict")]
    [InlineData("tolerant")]
    public void A_Node_With_An_Identity_That_Has_Not_Announced_Is_Absent_Whatever_The_Mode(string mode)
    {
        var problem = Assert.Single(Check(JobOn("node-c"), Held(mode)));

        Assert.Equal((BindingCauses.Offline, false), (problem.Cause, problem.Blocking));
    }

    [Theory]
    [InlineData("node-off")]
    [InlineData("watch")]
    public void A_Disabled_Identity_And_A_Monitors_Name_Are_No_Node(string name)
    {
        Announce("node-a", SourceBrick());

        var problem = Assert.Single(Check(JobOn(name)));

        Assert.Equal((BindingCauses.UnknownNode, true), (problem.Cause, problem.Blocking));
    }

    [Fact]
    public void A_Node_That_Does_Not_Offer_The_Brick_Is_Refused_By_Name()
    {
        Announce("node-a", SourceBrick());
        Announce("node-b");

        var problem = Assert.Single(Check(JobOn("node-b")));

        Assert.Equal((BindingCauses.NotOffered, true), (problem.Cause, problem.Blocking));
        Assert.Contains("node-b", problem.Message);
    }

    [Fact]
    public void A_Node_With_No_Connected_Replica_Is_Offline_And_Does_Not_Block()
    {
        Announce("node-a", SourceBrick());
        _registry.Disconnected("c-node-a");

        var problem = Assert.Single(Check(Job));

        Assert.Equal((BindingCauses.Offline, false), (problem.Cause, problem.Blocking));
    }

    [Fact]
    public void The_Runner_Is_Never_Named_And_A_Branch_The_Job_Lacks_Is_Refused()
    {
        Announce("node-a", SourceBrick());
        var runner = new NodeAnnouncement("runner-1", "", [], NodeRole.Runner, [], InstanceId: Guid.NewGuid());
        _registry.Announce("c-runner-1", "runner-1", runner, ["crm"], () => { });
        _inventory.Announced("runner-1", "runner-1", runner, ["crm"]);
        var bindings = Held();
        bindings["sink"] = new Binding("runner-1", "sink");
        bindings["ghost"] = new Binding("node-a", "ghost");

        var problems = Check(Job, bindings);

        Assert.Equal(
            [("sink", BindingCauses.NotABrick), ("ghost", BindingCauses.NoBranch)],
            problems.Select(p => (p.Alias, p.Cause)));
        Assert.All(problems, p => Assert.True(p.Blocking));
    }

    private PlanBuilder Planner() => new(_inventory, new BrickCatalog(_inventory),
        new PlanRegistry(new DefaultFlowControlService(new FlowControlOptions())), _rights);

    [Fact]
    public async Task A_Bound_Branch_That_References_No_Brick_Is_Never_Sent_To_The_Runner()
    {
        Announce("node-a", SourceBrick());
        var announcement = new NodeAnnouncement("runner-1", "", [], NodeRole.Runner, [], InstanceId: Guid.NewGuid());
        _registry.Announce("c-runner-1", "runner-1", announcement, ["crm"], () => { });
        _inventory.Announced("runner-1", "runner-1", announcement, ["crm"]);
        var placer = new AutoPlacer(new BrickCatalog(_inventory), _inventory, Planner(), _rights);
        var text = "# P\ncustomers:\n  input: sqlite:b\nsink:\n  from: customers\n  output: csv:out.csv\n";

        var plan = placer.Plan("p", text, new Dictionary<string, Binding> { ["customers"] = new Binding("node-a", "customers") },
            _inventory.Snapshot().Single(n => n.Name == "runner-1"));

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
        }, problems);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void A_Mode_That_Is_None_Of_The_Four_Is_Refused_By_Name()
    {
        Announce("node-a", SourceBrick());

        var problem = Assert.Single(Check(Job, Held("loose")));

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

        var unpinned = Assert.Single(Check(Job, new() { ["customers"] = new Binding("", null, mode) }));
        var pinned = Check(Job, new() { ["customers"] = new Binding("", null, mode, new BindingPin("v", "{}")) });

        Assert.Equal((BindingCauses.Unchecked, true), (unpinned.Cause, unpinned.Blocking));
        Assert.Empty(pinned);
    }

    [Fact]
    public void A_Pin_Without_The_Contract_Its_Mode_Compares_Is_Unchecked_But_A_Tolerant_One_Needs_None()
    {
        Announce("node-a", SourceBrick());

        Assert.Equal(BindingCauses.Unchecked, Assert.Single(Check(Job, new() { ["customers"] = new Binding("", null, "compatible", new BindingPin("v")) })).Cause);
        Assert.Empty(Check(Job, new() { ["customers"] = new Binding("", null, "tolerant", new BindingPin("v")) }));
    }

    [Fact]
    public void A_Binding_In_Another_Mode_Names_The_Node_Or_The_Brick_That_Is_Missing()
    {
        Announce("node-a", SourceBrick());
        var tolerant = new Dictionary<string, Binding> { ["customers"] = new Binding("", null, "tolerant", new BindingPin("v")) };

        Assert.Equal(BindingCauses.UnknownNode, Assert.Single(Check(JobOn("node-z"), tolerant)).Cause);
        Assert.Equal(BindingCauses.NotOffered, Assert.Single(Check(JobOn("node-a", "gone"), tolerant)).Cause);
    }

    [Fact]
    public async Task A_Binding_Is_Kept_Beside_The_Job_As_Its_Mode_And_Pin_And_Read_Back_With_Its_Node_And_Brick()
    {
        Announce("node-a", SourceBrick());
        var library = Library();
        await library.SaveAsync("p", Job, "create", Ct);

        await library.SaveBindingsAsync("p", new Dictionary<string, Binding> { ["customers"] = new Binding("", null, "tolerant", new BindingPin("v")) }, "bind", Ct);

        var document = (await library.GetAsync("p", Ct))!;
        Assert.Equal(new Binding("node-a", "customers", "tolerant", new BindingPin("v")), document.Bindings["customers"]);
        Assert.Equal(Job, document.Yaml);
        var written = await File.ReadAllTextAsync(Path.Combine(_dir, "library", "pipelines", "p", "bindings.json"), Ct);
        Assert.Contains("\"mode\": \"tolerant\"", written);
        Assert.DoesNotContain("node-a", written);
        Assert.DoesNotContain("\"brick\"", written);
    }

    [Fact]
    public async Task Bindings_Saved_After_The_Job_Do_Not_Change_The_Version_Of_The_Job()
    {
        var library = Library();
        await library.SaveAsync("p", Job, "create", Ct);
        await library.SaveBindingsAsync("p", Held("tolerant", new BindingPin("v1")), "bind", Ct);
        var jobCommit = (await library.GetAsync("p", Ct))!.Commit;

        await library.SaveBindingsAsync("p", Held("tolerant", new BindingPin("v2")), "move", Ct);

        var document = (await library.GetAsync("p", Ct))!;
        Assert.Equal("v2", document.Bindings["customers"].Pin!.Version);
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
        await library.SaveBindingsAsync("p", Held("tolerant", new BindingPin("v")), "bind", Ct);

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
        await File.WriteAllTextAsync(Path.Combine(seeds, "p.bindings.json"), """{ "customers": { "mode": "tolerant", "pin": { "version": "v" } } }""", Ct);
        var library = new PipelineLibrary(new CoordinatorServerOptions
        {
            StateDir = _dir, LibrarySeed = seeds, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret",
        });

        Assert.Equal(new Binding("node-a", "customers", "tolerant", new BindingPin("v")), (await library.GetAsync("p", Ct))!.Bindings["customers"]);
    }

    [Fact]
    public async Task A_Save_Sets_The_Bindings_It_Names_And_Keeps_The_Others_In_One_Commit()
    {
        var library = Library();
        await library.SaveAsync("p", Job, "create", Ct,
            bindings: new Dictionary<string, Binding> { ["customers"] = new Binding("", null, "contract", new BindingPin("v", "{}")), ["sink"] = new Binding("node-z", "sink") });
        var before = (await library.HistoryAsync("p", Ct)).Count;

        await library.SaveAsync("p", Job + "# edited\n", "edit", Ct, bindings: Held("tolerant", new BindingPin("w")));

        var bindings = (await library.GetAsync("p", Ct))!.Bindings;
        Assert.Equal(("tolerant", "node-z"), (bindings["customers"].Mode, bindings["sink"].Node));
        Assert.Equal(before + 1, (await library.HistoryAsync("p", Ct)).Count);
    }

    [Fact]
    public void The_Designers_Check_Takes_The_Brick_A_Reference_Names_And_No_Other()
    {
        Announce("node-a", SourceBrick());
        Announce("node-b", SourceBrick());
        var validator = new DesignValidator(new BrickCatalog(_inventory), null!);
        YamlDotNet.RepresentationModel.YamlMappingNode Branch(string text) => (YamlDotNet.RepresentationModel.YamlMappingNode)DtPipe.Jobs.JobYaml.FromJson(JsonNode.Parse(text));

        Assert.Equal("node-b/customers", validator.BrickOf("customers", Branch("""{"input":"brick:node-b/customers"}"""))!.Key);
        Assert.Equal("node-a/customers", validator.BrickOf("customers", Branch("""{"input":"brick:node-a/customers"}"""))!.Key);
        Assert.Null(validator.BrickOf("customers", Branch(Source)));
        Assert.Null(validator.BrickOf("customers", Branch("""{"input":"brick:node-a/customers","limit":10}""")));
    }
}
