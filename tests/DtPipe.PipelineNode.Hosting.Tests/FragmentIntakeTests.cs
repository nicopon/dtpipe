using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using Xunit;
using YamlDotNet.RepresentationModel;

namespace DtPipe.PipelineNode.Hosting.Tests;

public class FragmentIntakeTests
{
    private const string Sink = """{ "output": "duck:${{WAREHOUSE}}", "provider-options": { "duck-writer": { "table": "dim" } } }""";

    private static YamlMappingNode? Declared(string id, BrickKind kind, string json = Sink) =>
        id == "dim" && kind == BrickKind.Sink ? (YamlMappingNode)JobYaml.FromJson(JsonNode.Parse(json)) : null;

    private static readonly FragmentUnit[] Units = [new("anonymize", FragmentUnitKind.Inbound), new("load", FragmentUnitKind.Sink, "dim", "anonymize")];

    private static FragmentSpec Planned(string? version = null) => new(
        "p@node-4", version ?? FragmentJob.VersionOf(FragmentJob.Compose(Units, (id, kind) => Declared(id, kind)).Yaml!), [], Units: Units);

    private static (string? Yaml, string? Reason) Resolve(FragmentSpec fragment, FragmentPolicy policy = FragmentPolicy.Bricks, FragmentGate? gate = null,
        FragmentJob.BranchOfBrick? branch = null) =>
        FragmentIntake.JobOf(fragment, policy, gate, branch ?? ((id, kind) => Declared(id, kind)), []);

    [Fact]
    public void A_data_node_composes_the_job_the_coordinator_planned_from_its_own_bricks()
    {
        var (yaml, reason) = Resolve(Planned());

        Assert.Null(reason);
        Assert.Equal(FragmentJob.Compose(Units, (id, kind) => Declared(id, kind)).Yaml, yaml);
    }

    [Fact]
    public void A_brick_that_changed_since_the_plan_is_refused_by_name()
    {
        var (yaml, reason) = Resolve(Planned(), branch: (id, kind) => Declared(id, kind, Sink.Replace("\"dim\"", "\"dim2\"")));

        Assert.Null(yaml);
        Assert.Contains("a brick of p@node-4 (dim) changed since the plan was made", reason);
    }

    [Fact]
    public void A_brick_the_node_does_not_offer_is_refused_by_name()
    {
        var (yaml, reason) = Resolve(Planned(), branch: (_, _) => null);

        Assert.Null(yaml);
        Assert.Contains("offers no sink brick 'dim'", reason);
    }

    [Fact]
    public void The_text_of_a_job_sent_as_units_is_never_what_runs()
    {
        var sent = Planned() with { Yaml = "evil:\n  input: csv:/etc/passwd\n" };

        var (yaml, _) = Resolve(sent);

        Assert.DoesNotContain("passwd", yaml);
    }

    [Fact]
    public void A_runner_composes_nothing_from_bricks_it_does_not_have()
    {
        var (yaml, reason) = Resolve(Planned(), FragmentPolicy.AnyFragment);

        Assert.Null(yaml);
        Assert.Contains("a runner has no brick", reason);
    }

    [Fact]
    public void A_runner_runs_a_job_as_written()
    {
        var (yaml, reason) = Resolve(new FragmentSpec("p@runner-1", "v", [], Yaml: "a:\n  input: generate:5\n"), FragmentPolicy.AnyFragment);

        Assert.Null(reason);
        Assert.Contains("generate:5", yaml);
    }

    [Fact]
    public void A_data_node_takes_no_job_as_written_unless_its_host_registered_a_gate()
    {
        var job = new FragmentSpec("p@node-1", "v", [], Yaml: "a:\n  input: csv:/etc/hosts\n");

        var strict = Resolve(job);
        var lax = Resolve(job, gate: (_, _) => null);
        var stricter = Resolve(job, gate: (_, _) => "no csv here");

        Assert.Null(strict.Yaml);
        Assert.Contains("by id", strict.Reason);
        Assert.Contains("csv:/etc/hosts", lax.Yaml);
        Assert.Equal("no csv here", stricter.Reason);
    }
}
