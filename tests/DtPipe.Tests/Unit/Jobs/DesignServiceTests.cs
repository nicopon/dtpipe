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
    private static readonly DesignService WithBricks = new(new Bricks(
        ("a/people", StepKinds.Source),
        ("b/archive", StepKinds.Sink)));

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
    public void Cards_Compose_Into_A_Job_That_References_Its_Bricks()
    {
        var result = WithBricks.Compose(Masking());

        Assert.Empty(result.Errors);
        Assert.Equal(
            """
            # Masked people
            # Copies people without their email.
            # review-by: data-team
            people:
              input: brick:a/people
            masked:
              from: people
              transformers:
              - type: overwrite
                mappings:
                  email: hidden
            archive:
              from: masked
              output: brick:b/archive

            """.Replace("\r\n", "\n"),
            result.Yaml);
        Assert.Equal(["people"], result.Steps.Single(s => s.Alias == "masked").DependsOn);

        // A reference is read the way the coordinator and the node read it, and shares its branch with nothing.
        var job = JobYaml.Parse(result.Yaml);
        var people = BrickRef.Of("people", (YamlMappingNode)job["people"])!;
        var archive = BrickRef.Of("archive", (YamlMappingNode)job["archive"])!;
        Assert.Equal(("a/people", false), (people.Key, people.IsSink));
        Assert.Equal(("b/archive", true), (archive.Key, archive.IsSink));
        Assert.Null(BrickRef.Misuse("people", (YamlMappingNode)job["people"]));
        Assert.Null(BrickRef.Misuse("archive", (YamlMappingNode)job["archive"]));
        Assert.Null(BrickRef.Of("masked", (YamlMappingNode)job["masked"]));
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
    public void A_Reference_That_Says_More_Or_Names_No_Offered_Brick_Stays_A_Raw_Branch_And_Composes_Back()
    {
        const string job = """
            says_more:
              input: brick:a/people
              transformers:
              - type: overwrite
                mappings:
                  email: hidden
            not_offered:
              input: brick:a/ghost
            wrong_way:
              input: brick:b/archive
            """;

        var result = WithBricks.Decompose(job);

        Assert.Equal(
            ["says_more:branch", "not_offered:branch", "wrong_way:branch"],
            result.Model.Steps.Select(s => $"{s.Alias}:{s.Kind}"));
        Assert.Equal(JobYaml.Serialize(JobYaml.Parse(job)), result.Yaml[result.Yaml.IndexOf('\n')..].TrimStart('\n'));
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

    /// <summary>
    /// A catalog of bricks offered here. Like the coordinator, it answers for a branch that is the reference to an offered
    /// brick in the right direction, and for nothing else.
    /// </summary>
    private sealed class Bricks(params (string Key, string Kind)[] offered) : IBrickLookup
    {
        public DesignBrick? Find(string key) => offered.Where(b => b.Key == key)
            .Select(b => new DesignBrick(b.Key, b.Kind, b.Key.Split('/')[0], "v1", b.Key, true)).FirstOrDefault();

        public YamlMappingNode BranchOf(DesignBrick brick) =>
            new() { { brick.Kind == StepKinds.Sink ? "output" : "input", BrickRef.Format(brick.Node!, brick.Key.Split('/')[1]) } };

        public DesignBrick? Match(YamlMappingNode branch) =>
            BrickRef.Of("", branch) is { } reference && Find(reference.Key) is { } brick
                && brick.Kind == (reference.IsSink ? StepKinds.Sink : StepKinds.Source) ? brick : null;
    }
}
