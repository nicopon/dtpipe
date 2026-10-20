using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using Xunit;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A node announces a source brick without its text, only its version, and a job reaches it by reference
/// (<c>input: brick:&lt;node&gt;/&lt;brick&gt;</c>): the catalog still lists it, the bindings hold only while a check pinned the version the
/// node announces now, and a reference never reaches dtpipe as written.
/// </summary>
public sealed class MinimalBrickTests : IDisposable
{
    private const string Text = """{"input":"sqlite:${{DB}}","provider-options":{"sqlite-reader":{"query":"SELECT 1"}}}""";
    private const string Job = "# P\ncustomers:\n  input: brick:node-a/customers\nsink:\n  from: customers\n  output: csv:out.csv\n";

    private static readonly Dictionary<string, Binding> NoBindings = [];

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "minimal-" + Guid.NewGuid().ToString("N"));
    private readonly ReplicaRegistry _registry = new(new ReplicaRegistryOptions { DisconnectGracePeriod = TimeSpan.FromHours(1) });
    private readonly NodeInventory _inventory;
    private readonly BrickCatalog _catalog;
    private readonly AutoPlacer _placer;

    public MinimalBrickTests()
    {
        _inventory = new NodeInventory(_registry);
        _catalog = new BrickCatalog(_inventory);
        Directory.CreateDirectory(_dir);
        var seed = Path.Combine(_dir, "rights.json");
        File.WriteAllText(seed, """{ "identities": [ { "clientId": "node-a", "name": "node-a", "description": "", "groups": ["crm"] } ], "matrix": {} }""");
        var rights = MemoryRightsStore.Rights(new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, "state"), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
        });
        _placer = new AutoPlacer(_catalog, _inventory, null!, rights);
    }

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    /// <summary>The brick as a node announces it: its version, never its text.</summary>
    private static BrickInfo Minimal(string text = Text) =>
        new("customers", BrickKind.Source, "Customers", "", null, null, null, BrickRules.VersionOf(BrickKind.Source, text));

    private void Announce(BrickInfo brick)
    {
        var announcement = new NodeAnnouncement("node-a", "", [], NodeRole.Data, [brick], InstanceId: Guid.NewGuid());
        _registry.Announce("c-a", "node-a", announcement, ["crm"], () => { });
        _inventory.Announced("node-a", "node-a", announcement, ["crm"]);
    }

    [Fact]
    public void A_Minimal_Brick_Is_Listed_By_Its_Version_And_Carries_No_Text()
    {
        var brick = Minimal();
        Announce(brick);

        var listed = Assert.Single(_catalog.List());

        Assert.Equal(("node-a/customers", brick.Version), (listed.Key, listed.Version));
        Assert.Null(brick.Branch);
        Assert.DoesNotContain("sqlite", listed.Yaml);
        Assert.DoesNotContain("DB", listed.Yaml);
        Assert.Equal("brick:node-a/customers", Assert.Single(_catalog.Catalog()).Branch["input"]!.GetValue<string>());
    }

    [Fact]
    public void A_Reference_Is_Never_Matched_As_A_Text()
    {
        Announce(Minimal());
        var branch = (YamlMappingNode)JobYaml.FromJson(JsonNode.Parse(Text));

        Assert.Empty(_catalog.MatchAll(branch));
    }

    [Fact]
    public void A_Referenced_Brick_Is_Unchecked_Until_A_Check_Pins_Its_Version_And_Changed_When_Its_Version_Moves()
    {
        var brick = Minimal();
        Announce(brick);

        Assert.Equal(BindingCauses.Unchecked, Assert.Single(_placer.Check(Job, NoBindings)).Cause);

        var pinned = new Dictionary<string, Binding> { ["customers"] = new Binding("", null, null, new BindingPin(brick.Version!)) };
        Assert.Empty(_placer.Check(Job, pinned));

        Announce(Minimal("""{"input":"sqlite:${{DB}}","provider-options":{"sqlite-reader":{"query":"SELECT 2"}}}"""));
        Assert.Equal(BindingCauses.BrickChanged, Assert.Single(_placer.Check(Job, pinned)).Cause);
    }

    [Fact]
    public void A_Reference_To_A_Brick_No_Node_Offers_Is_Refused_By_Name()
    {
        Announce(Minimal());

        var problem = Assert.Single(_placer.Check(Job.Replace("node-a/customers", "node-a/absent"), NoBindings));

        Assert.Equal(BindingCauses.NotOffered, problem.Cause);
        Assert.Contains("absent", problem.Message);
    }

    [Fact]
    public void A_Reference_That_Says_More_Is_A_Misuse()
    {
        Announce(Minimal());

        var problem = Assert.Single(_placer.Check(Job.Replace("customers:\n  input: brick:node-a/customers", "customers:\n  input: brick:node-a/customers\n  limit: 5"), NoBindings));

        Assert.Equal(BindingCauses.NotABrick, problem.Cause);
    }

    [Fact]
    public void The_Version_Of_A_Fragment_Is_A_Function_Of_The_Versions_Of_Its_Bricks()
    {
        FragmentUnit[] units = [new("customers", FragmentUnitKind.SourceSend, "customers")];

        string? Of(string v) => FragmentJob.VersionOf(units, (_, _) => v);

        Assert.Equal(Of("aaa"), Of("aaa"));
        Assert.NotEqual(Of("aaa"), Of("bbb"));
    }

    [Fact]
    public void A_Referenced_Source_Is_Placed_On_Its_Node_Whatever_The_Bindings_Say()
    {
        Announce(Minimal());

        var expanded = BrickReferences.Expand(Job, new Dictionary<string, Binding>());

        Assert.Equal(("node-a", "customers"), (expanded["customers"].Node, expanded["customers"].Brick));
        Assert.Equal("node-a/customers", _placer.BrickAliases(Job)["customers"]);
    }

    [Fact]
    public async Task A_Reference_Never_Reaches_The_Runner_Of_A_Check()
    {
        Announce(Minimal());
        var validator = new DesignValidator(_catalog, null!);

        var check = await validator.ValidateAsync("customers:\n  input: brick:node-a/absent\n", new Dictionary<string, Binding>(), TestContext.Current.CancellationToken);

        Assert.False(check.Ok);
        Assert.Contains("node-a/absent", check.Report);
    }
}
