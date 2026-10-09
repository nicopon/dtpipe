using System.Text.Json.Nodes;
using DtPipe.Jobs;
using Xunit;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Tests.Unit.Jobs;

/// <summary>
/// The designer's cards and the job text, both ways: the one model dtpipe studio and the
/// coordinator's server compose with.
/// </summary>
public sealed class DesignServiceTests
{
    private const string SourceYaml = "input: pg:Host=a\nprovider-options:\n  pg:\n    query: SELECT * FROM people\n";
    private const string SinkYaml = "output: duck:archive.duckdb\nprovider-options:\n  duck-writer:\n    table: people\n";

    private static readonly DesignService WithBricks = new(new Bricks(
        ("a/people", StepKinds.Source, SourceYaml),
        ("b/archive", StepKinds.Sink, SinkYaml)));

    private static readonly DesignService NoBricks = new(IBrickLookup.None);

    private static JsonArray Overwrite() =>
        new(new JsonObject { ["type"] = "overwrite", ["mappings"] = new JsonObject { ["email"] = "hidden" } });

    private static DesignModel Masking() => new(
        "Masked people", "Copies people without their email.", ["review-by: data-team"],
        [
            new DesignStep("people", StepKinds.Source, "a/people"),
            new DesignStep("masked", StepKinds.Transform, From: ["people"], Transformers: Overwrite()),
            new DesignStep("archive", StepKinds.Sink, "b/archive", From: ["masked"]),
        ]);

    [Fact]
    public void Cards_Compose_Into_A_Plain_Job_With_Its_Header()
    {
        var result = WithBricks.Compose(Masking());

        Assert.Empty(result.Errors);
        Assert.Equal(
            """
            # Masked people
            # Copies people without their email.
            # review-by: data-team
            people:
              input: pg:Host=a
              provider-options:
                pg:
                  query: SELECT * FROM people
            masked:
              from: people
              transformers:
              - type: overwrite
                mappings:
                  email: hidden
            archive:
              from: masked
              output: duck:archive.duckdb
              provider-options:
                duck-writer:
                  table: people

            """.Replace("\r\n", "\n"),
            result.Yaml);
        Assert.Equal(["people"], result.Steps.Single(s => s.Alias == "masked").DependsOn);
    }

    [Fact]
    public void A_Composed_Job_Decomposes_Into_The_Same_Cards()
    {
        var composed = WithBricks.Compose(Masking());

        var back = WithBricks.Decompose(composed.Yaml);

        Assert.Empty(back.Errors);
        Assert.Equal("Masked people", back.Model.Title);
        Assert.Equal("Copies people without their email.", back.Model.Description);
        Assert.Equal(["review-by: data-team"], back.Model.Directives);
        Assert.Equal(
            ["people:source:a/people", "masked:transform:", "archive:sink:b/archive"],
            back.Model.Steps.Select(s => $"{s.Alias}:{s.Kind}:{s.Brick}"));
        Assert.Equal(composed.Yaml, back.Yaml);
    }

    [Fact]
    public void A_Branch_Around_Two_Bricks_Is_Taken_Apart_And_Keeps_Its_Alias_On_Its_Work()
    {
        const string job = """
            copy:
              input: pg:Host=a
              output: duck:archive.duckdb
              transformers:
              - type: overwrite
                mappings:
                  email: hidden
              provider-options:
                pg:
                  query: SELECT * FROM people
                duck-writer:
                  table: people
            """;

        var result = WithBricks.Decompose(job);

        Assert.Empty(result.Errors);
        Assert.Equal(
            ["copy_source:source:a/people:", "copy:transform::copy_source", "copy_sink:sink:b/archive:copy"],
            result.Model.Steps.Select(s => $"{s.Alias}:{s.Kind}:{s.Brick}:{string.Join(',', s.From ?? [])}"));
    }

    [Fact]
    public void Without_Bricks_A_Job_Becomes_Raw_Branches_Sql_And_Merge_Cards_And_Composes_Back()
    {
        const string job = """
            a:
              input: csv:a.csv
            b:
              input: csv:b.csv
            both:
              from: a,b
              provider-options:
                merge: {}
            q:
              from: both
              ref:
              - a
              provider-options:
                sql:
                  query: SELECT * FROM both
            out:
              from: q
              output: csv:out.csv
            """;

        var result = NoBricks.Decompose(job);

        Assert.Empty(result.Errors);
        Assert.Equal(
            ["a:branch", "b:branch", "both:merge", "q:sql", "out:branch"],
            result.Model.Steps.Select(s => $"{s.Alias}:{s.Kind}"));
        Assert.Equal("SELECT * FROM both", result.Model.Steps.Single(s => s.Alias == "q").Query);
        Assert.Equal(
            JobYaml.Serialize(JobYaml.Parse(job)),
            result.Yaml[result.Yaml.IndexOf('\n')..].TrimStart('\n'));
    }

    [Fact]
    public void A_Brick_Key_Without_Bricks_Is_An_Error_Not_A_Guess()
    {
        var result = NoBricks.Compose(Masking());

        Assert.Contains(result.Errors, e => e.Contains("'people'") && e.Contains("a/people"));
        Assert.False(result.Valid);
    }

    [Theory]
    [InlineData("1st", "is not a usable alias")]
    [InlineData("dup", "Two steps are named 'dup'")]
    [InlineData("dangling", "reads 'ghost', which no step defines")]
    [InlineData("cycle", "through a cycle")]
    [InlineData("nowhere", "leads nowhere")]
    public void Wiring_Mistakes_Are_Named(string shape, string expected)
    {
        DesignStep Raw(string alias, string yaml, params string[] from) => new(alias, StepKinds.Branch, From: from, Yaml: yaml);
        IReadOnlyList<DesignStep> steps = shape switch
        {
            "1st" => [Raw("1st", "input: csv:a.csv\noutput: csv:b.csv\n")],
            "dup" => [Raw("dup", "input: csv:a.csv\noutput: csv:b.csv\n"), Raw("dup", "input: csv:a.csv\noutput: csv:c.csv\n")],
            "dangling" => [Raw("t", "from: ghost\noutput: csv:b.csv\n")],
            "cycle" => [Raw("x", "from: y\n"), Raw("y", "from: x\n")],
            _ => [Raw("lonely", "input: csv:a.csv\n")],
        };

        var result = NoBricks.Compose(new DesignModel("T", null, null, steps));

        Assert.Contains(result.Errors, e => e.Contains(expected));
    }

    [Fact]
    public void The_Header_Separates_Directives_From_Prose()
    {
        var (title, description, directives) = DesignService.ReadHeader(
            "# Title\n# First line\n# lab-check: SELECT 1\n# second line: not a directive\nmain:\n  input: csv:a.csv\n");

        Assert.Equal("Title", title);
        Assert.Equal("First line second line: not a directive", description);
        Assert.Equal(["lab-check: SELECT 1"], directives);
    }

    /// <summary>A catalog over branches declared here, matched by their text the way the coordinator matches by content.</summary>
    private sealed class Bricks(params (string Key, string Kind, string Yaml)[] declared) : IBrickLookup
    {
        private static string Text(YamlNode node) => JobYaml.Serialize(new YamlMappingNode { { "b", node } });

        private IEnumerable<(DesignBrick Brick, YamlMappingNode Branch)> All() => declared.Select(d =>
            (new DesignBrick(d.Key, d.Kind, d.Key.Split('/')[0], "v1", d.Key, true), (YamlMappingNode)JobYaml.Parse($"b:\n  {d.Yaml.TrimEnd().Replace("\n", "\n  ")}\n")["b"]));

        public DesignBrick? Find(string key) => All().FirstOrDefault(b => b.Brick.Key == key).Brick;

        public YamlMappingNode BranchOf(DesignBrick brick) => JobYaml.CloneBranch(All().First(b => b.Brick.Key == brick.Key).Branch);

        public DesignBrick? Match(YamlMappingNode branch)
        {
            var bare = new YamlMappingNode(branch.Children.Where(kv => ((YamlScalarNode)kv.Key).Value != "from"));
            return All().FirstOrDefault(b => Text(b.Branch) == Text(bare)).Brick;
        }

        public DesignBrick? Contained(YamlMappingNode branch, string kind)
        {
            var endpoint = kind == StepKinds.Source ? "input" : "output";
            var options = JobYaml.Mapping(branch, "provider-options");
            return All().Where(b => b.Brick.Kind == kind && JobYaml.Scalar(b.Branch, endpoint) == JobYaml.Scalar(branch, endpoint))
                .FirstOrDefault(b => JobYaml.Mapping(b.Branch, "provider-options")!.Children.All(kv =>
                    options is not null && options.Children.TryGetValue(kv.Key, out var mine) && Text(mine) == Text(kv.Value))).Brick;
        }
    }
}
