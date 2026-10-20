using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using Xunit;
using YamlDotNet.RepresentationModel;

namespace DtPipe.PipelineNode.Hosting.Tests;

public class FragmentJobTests
{
    private const string CrmSource = """{ "input": "sqlite:${{CRM}}", "provider-options": { "sqlite-reader": { "query": "SELECT * FROM customers" } } }""";
    private const string WarehouseSink = """{ "output": "duck:${{WAREHOUSE}}", "provider-options": { "duck-writer": { "table": "dim" } } }""";

    private static YamlMappingNode? Declared(string id, BrickKind kind) => (id, kind) switch
    {
        ("customers", BrickKind.Source) => (YamlMappingNode)JobYaml.FromJson(System.Text.Json.Nodes.JsonNode.Parse(CrmSource)),
        ("dim", BrickKind.Sink) => (YamlMappingNode)JobYaml.FromJson(System.Text.Json.Nodes.JsonNode.Parse(WarehouseSink)),
        _ => null,
    };

    private static (string? Yaml, string? Error) Compose(params FragmentUnit[] units) => FragmentJob.Compose(units, Declared);

    [Fact]
    public void A_version_is_the_lowercase_sha256_of_the_composed_text()
    {
        Assert.Equal("6e97d1eecab3903be5d7e57b5d9b31adc4eb6be9bbdd3daf2c3a707f79ff53a9", FragmentJob.VersionOf("src:\n  input: sqlite:a\n  output: arrow:-\n"));
    }

    [Fact]
    public void The_version_of_a_fragment_of_bricks_is_the_hash_of_its_units_and_the_versions_of_their_bricks()
    {
        FragmentUnit[] units = [new("src", FragmentUnitKind.SourceSend, "src"), new("load", FragmentUnitKind.Sink, "dim", "src")];

        Assert.Equal("5bfb4330239ee1f49973beb37ab812972d5207a6667cc373a2240c80fe8d50cb", FragmentJob.VersionOf(units, (id, _) => id == "src" ? "aaaaaaaaaaaa" : "bbbbbbbbbbbb"));
    }

    [Fact]
    public void A_source_that_sends_its_rows_and_a_relay_compose_in_the_order_given()
    {
        var (yaml, error) = Compose(
            new FragmentUnit("customers", FragmentUnitKind.Source, "customers"),
            new FragmentUnit("customers__to__runner", FragmentUnitKind.Relay, From: "customers"));

        Assert.Null(error);
        var branches = JobYaml.Branches(JobYaml.Parse(yaml!)).ToList();
        Assert.Equal(["customers", "customers__to__runner"], branches.Select(b => b.Alias));
        Assert.Equal("customers", JobYaml.Scalar(branches[1].Branch, "from"));
        Assert.Equal(BrickRules.ArrowEndpoint, JobYaml.Scalar(branches[1].Branch, "output"));
        Assert.False(JobYaml.Has(branches[0].Branch, "output"));
    }

    [Fact]
    public void A_source_sent_straight_to_an_edge_gains_its_arrow_output_and_stays_a_brick()
    {
        var (yaml, _) = Compose(new FragmentUnit("customers", FragmentUnitKind.SourceSend, "customers"));

        var branch = JobYaml.Branches(JobYaml.Parse(yaml!)).Single().Branch;
        Assert.Equal(BrickRules.ArrowEndpoint, JobYaml.Scalar(branch, "output"));
        var form = BrickRules.FormOf(branch);
        Assert.Equal(BrickRules.Declared(BrickKind.Source, CrmSource), form);
    }

    [Fact]
    public void A_sink_reads_the_inbound_edge_it_names_and_stays_a_brick()
    {
        var (yaml, error) = Compose(
            new FragmentUnit("anonymize", FragmentUnitKind.Inbound),
            new FragmentUnit("load", FragmentUnitKind.Sink, "dim", "anonymize"));

        Assert.Null(error);
        var branches = JobYaml.Branches(JobYaml.Parse(yaml!)).ToDictionary(b => b.Alias, b => b.Branch);
        Assert.Equal(BrickRules.ArrowEndpoint, JobYaml.Scalar(branches["anonymize"], "input"));
        Assert.Equal("anonymize", JobYaml.Scalar(branches["load"], "from"));
        Assert.Equal(BrickRules.Declared(BrickKind.Sink, WarehouseSink), BrickRules.FormOf(branches["load"]));
    }

    [Fact]
    public void Both_sides_compose_the_same_bytes_and_so_the_same_version()
    {
        FragmentUnit[] units =
        [
            new("anonymize", FragmentUnitKind.Inbound),
            new("load", FragmentUnitKind.Sink, "dim", "anonymize"),
        ];

        var coordinator = FragmentJob.Compose(units, Declared).Yaml!;
        var node = FragmentJob.Compose([.. units], Declared).Yaml!;

        Assert.Equal(coordinator, node);
        Assert.Equal(FragmentJob.VersionOf(coordinator), FragmentJob.VersionOf(node));
    }

    [Fact]
    public void A_brick_that_changed_gives_another_version()
    {
        FragmentUnit[] units = [new("customers", FragmentUnitKind.SourceSend, "customers")];
        var before = FragmentJob.Compose(units, Declared).Yaml!;
        var after = FragmentJob.Compose(units, (_, _) => (YamlMappingNode)JobYaml.FromJson(System.Text.Json.Nodes.JsonNode.Parse(
            CrmSource.Replace("SELECT *", "SELECT customer_id")))).Yaml!;

        Assert.NotEqual(FragmentJob.VersionOf(before), FragmentJob.VersionOf(after));
    }

    [Fact]
    public void An_unknown_brick_id_is_refused_by_name()
    {
        var (yaml, error) = Compose(new FragmentUnit("x", FragmentUnitKind.Source, "no-such-brick"));

        Assert.Null(yaml);
        Assert.Contains("no-such-brick", error);
    }

    [Fact]
    public void A_brick_of_the_other_kind_is_no_brick_of_this_piece()
    {
        Assert.Contains("no source brick 'dim'", Compose(new FragmentUnit("x", FragmentUnitKind.Source, "dim")).Error);
        Assert.Contains("no sink brick 'customers'", Compose(new FragmentUnit("a", FragmentUnitKind.Inbound), new FragmentUnit("x", FragmentUnitKind.Sink, "customers", "a")).Error);
    }

    [Fact]
    public void A_piece_that_is_none_of_the_vocabulary_is_refused()
    {
        Assert.Contains("unknown kind", Compose(new FragmentUnit("x", (FragmentUnitKind)99)).Error);
    }

    [Fact]
    public void A_relay_or_a_sink_reads_another_piece_of_the_fragment()
    {
        Assert.Contains("no other piece", Compose(new FragmentUnit("r", FragmentUnitKind.Relay, From: "ghost")).Error);
        Assert.Contains("no other piece", Compose(new FragmentUnit("r", FragmentUnitKind.Relay, From: "r")).Error);
        Assert.Contains("reads no piece", Compose(new FragmentUnit("a", FragmentUnitKind.Inbound), new FragmentUnit("x", FragmentUnitKind.Sink, "dim")).Error);
    }

    [Theory]
    [InlineData("../escape")]
    [InlineData("a b")]
    [InlineData("1st")]
    [InlineData("")]
    public void A_key_is_a_plain_alias(string key)
    {
        Assert.Contains("is not an alias", Compose(new FragmentUnit(key, FragmentUnitKind.Inbound)).Error);
    }

    [Fact]
    public void Two_pieces_cannot_share_a_key()
    {
        Assert.Contains("two pieces", Compose(new FragmentUnit("a", FragmentUnitKind.Inbound), new FragmentUnit("a", FragmentUnitKind.Inbound)).Error);
    }

    [Fact]
    public void A_source_that_already_writes_somewhere_cannot_also_be_sent_to_an_edge()
    {
        var (yaml, error) = FragmentJob.Compose([new FragmentUnit("customers", FragmentUnitKind.SourceSend, "customers")],
            (_, _) => (YamlMappingNode)JobYaml.FromJson(System.Text.Json.Nodes.JsonNode.Parse("""{ "input": "a:b", "output": "csv:x" }""")));

        Assert.Null(yaml);
        Assert.Contains("already writes", error);
    }
}
