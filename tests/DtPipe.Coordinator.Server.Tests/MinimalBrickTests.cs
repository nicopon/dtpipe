using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using Xunit;
using YamlDotNet.RepresentationModel;
using DtPipe.Tests.Shared;

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
    private static BrickInfo Minimal(string text = Text) => TestBricks.Announced("customers", BrickKind.Source, text, "Customers");

    private static BrickInfo MinimalSink() => TestBricks.Announced("mirror", BrickKind.Sink, """{"output":"sqlite:${{DB}}","provider-options":{"sqlite-writer":{"table":"t"}}}""");

    private void Announce(params BrickInfo[] bricks)
    {
        var announcement = new NodeAnnouncement("node-a", "", [], NodeRole.Data, bricks, InstanceId: Guid.NewGuid());
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
        Assert.DoesNotContain("sqlite", listed.Yaml);
        Assert.DoesNotContain("DB", listed.Yaml);
        Assert.Equal("brick:node-a/customers", Assert.Single(_catalog.Catalog()).Branch["input"]!.GetValue<string>());
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

    private const string SinkJob = "customers:\n  input: brick:node-a/customers\nmirror:\n  from: customers\n  output: brick:node-a/mirror\n";

    [Fact]
    public void A_Sink_Is_Referenced_By_Its_Output_And_Pinned_At_The_Version_It_Was_Checked_At()
    {
        var (source, sink) = (Minimal(), MinimalSink());
        Announce(source, sink);

        var reference = BrickRef.Of("mirror", (YamlMappingNode)JobYaml.Parse(SinkJob).Children[new YamlScalarNode("mirror")]);
        Assert.Equal(("node-a/mirror", true), (reference!.Key, reference.IsSink));
        Assert.Equal("brick:node-a/mirror", Assert.Single(_catalog.Catalog(), b => b.Id == "mirror").Branch["output"]!.GetValue<string>());
        Assert.Equal(BindingCauses.Unchecked, Assert.Single(_placer.Check(SinkJob, new Dictionary<string, Binding> { ["customers"] = new("", null, null, new BindingPin(source.Version)) })).Cause);

        // A strict binding keeps the version, never the contract the sink was judged against.
        var pins = new Dictionary<string, CheckedPin> { ["mirror"] = new("mirror", new BindingPin(sink.Version, "{}")) };
        var bindings = Bindings.Pinned(BrickReferences.Expand(SinkJob, new Dictionary<string, Binding>()), pins, new HashSet<string> { "mirror" });
        Assert.Equal(new BindingPin(sink.Version), bindings["mirror"].Pin);
        Assert.Empty(_placer.Check(SinkJob, new Dictionary<string, Binding> { ["customers"] = new("", null, null, new BindingPin(source.Version)), ["mirror"] = bindings["mirror"] }));
    }

    [Theory]
    [InlineData("mirror:\n  from: customers\n  output: brick:node-a/customers\n")]
    [InlineData("mirror:\n  from: customers\n  output: brick:node-a/mirror\n  limit: 5\n")]
    [InlineData("mirror:\n  output: brick:node-a/mirror\n")]
    [InlineData("mirror:\n  from: customers,mirror2\n  output: brick:node-a/mirror\n")]
    public void A_Reference_That_Writes_Where_It_Cannot_Or_Reads_Nothing_Is_Refused_By_Name(string sink)
    {
        Announce(Minimal(), MinimalSink());
        var job = "customers:\n  input: brick:node-a/customers\nmirror2:\n  from: customers\n  output: csv:out.csv\n" + sink;

        var problem = Assert.Single(_placer.Check(job, new Dictionary<string, Binding> { ["customers"] = new("", null, null, new BindingPin(Minimal().Version)) }));

        Assert.Equal(("mirror", BindingCauses.NotABrick), (problem.Alias, problem.Cause));
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
