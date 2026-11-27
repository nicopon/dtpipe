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
        "Masked people\nCopies people without their email.\nreview-by: data-team",
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
        Assert.Equal("Masked people\nCopies people without their email.\nreview-by: data-team", back.Model.Header);
        Assert.Equal(
            ["people:source:a/people", "masked:transform:", "archive:sink:b/archive"],
            back.Model.Steps.Select(s => $"{s.Alias}:{s.Kind}:{s.Brick}"));
        Assert.Equal(composed.Yaml, back.Yaml);
    }

    [Fact]
    public void A_Transformers_Settings_Come_Back_As_The_Text_Dtpipe_Reads()
    {
        var transformers = new JsonArray(new JsonObject
        {
            ["type"] = "overwrite",
            ["mappings"] = new JsonObject { ["email"] = "~", ["phone"] = null, ["age"] = 5 },
        });
        var composed = WithBricks.Compose(new DesignModel("T",
        [
            new DesignStep("people", StepKinds.Source, "a/people"),
            new DesignStep("masked", StepKinds.Transform, From: ["people"], Transformers: transformers),
            new DesignStep("archive", StepKinds.Sink, "b/archive", From: ["masked"]),
        ]));

        var back = WithBricks.Decompose(composed.Yaml).Model.Steps.Single(s => s.Alias == "masked").Transformers!;

        Assert.Equal("""[{"type":"overwrite","mappings":{"email":"~","phone":"","age":"5"}}]""", back.ToJsonString());
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
        Assert.Null(result.Model.Header);
        Assert.Equal(JobYaml.Serialize(JobYaml.Parse(job)), result.Yaml);
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
        Assert.Null(result.Model.Header);
        Assert.Equal(JobYaml.Serialize(JobYaml.Parse(job)), result.Yaml);
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

        var result = NoBricks.Compose(new DesignModel("T", steps));

        Assert.Contains(result.Errors, e => e.Contains(expected));
    }

    [Fact]
    public void The_Header_Keeps_Comments_Verbatim_Without_Parsing_Or_Wrapping()
    {
        const string job = """
            # First line: kept as-is
            #
            #   indented line with more than ninety-six characters so we verify nothing wraps or collapses spaces across lines
            # lab-check: SELECT count(*) FROM customers
            main:
              input: csv:a.csv
              output: csv:b.csv
            """;

        var decomposed = NoBricks.Decompose(job);

        Assert.Equal(
            "First line: kept as-is\n\n  indented line with more than ninety-six characters so we verify nothing wraps or collapses spaces across lines\nlab-check: SELECT count(*) FROM customers",
            decomposed.Model.Header);
        Assert.Equal(job.Replace("\r\n", "\n") + "\n", decomposed.Yaml);
    }

    [Fact]
    public void Branch_Comments_Under_Alias_Round_Trip_And_Scalars_Are_Ignored()
    {
        const string job = """
            # Job header
            a:
              # Source branch comment
              # Second line
              input: csv:a.csv
            # Between-branch comment is not attached
            q:
              # DuckDB step
              from: a
              provider-options:
                sql:
                  query: |-
                    # Not a YAML comment
                    SELECT * FROM a
            out:
              from: q
              # Mid-branch comment is not attached
              output: csv:out.csv
            """;

        var decomposed = NoBricks.Decompose(job);

        Assert.Equal("Job header", decomposed.Model.Header);
        Assert.Equal("Source branch comment\nSecond line", decomposed.Model.Steps.Single(s => s.Alias == "a").Comment);
        Assert.Equal("DuckDB step", decomposed.Model.Steps.Single(s => s.Alias == "q").Comment);
        Assert.Null(decomposed.Model.Steps.Single(s => s.Alias == "out").Comment);

        var recomposed = NoBricks.Decompose(decomposed.Yaml);
        Assert.Equal("Source branch comment\nSecond line", recomposed.Model.Steps.Single(s => s.Alias == "a").Comment);
        Assert.Equal("DuckDB step", recomposed.Model.Steps.Single(s => s.Alias == "q").Comment);
        Assert.Contains("  # Source branch comment\n  # Second line\n  input: csv:a.csv", decomposed.Yaml);
        Assert.Contains("  # DuckDB step\n  from: a", decomposed.Yaml);
    }

    [Theory]
    [InlineData("\r")]
    [InlineData("\u0085")]
    [InlineData("\u2028")]
    [InlineData("\u2029")]
    public void Comments_With_Yaml_Line_Breaks_Never_Break_Out_Of_Comments(string lineBreak)
    {
        var model = new DesignModel(
            $"header{lineBreak}injected:{lineBreak}  input: csv:secret.csv",
            [
                new DesignStep("a", StepKinds.Source, "a/people", Comment: $"step{lineBreak}output: csv:pwn.csv"),
                new DesignStep("b", StepKinds.Sink, "b/archive", From: ["a"]),
            ]);

        var composed = WithBricks.Compose(model);
        var back = WithBricks.Decompose(composed.Yaml);

        Assert.Empty(composed.Errors);
        Assert.Equal(["a", "b"], back.Model.Steps.Select(s => s.Alias));
        Assert.DoesNotContain("\ninjected:", composed.Yaml);
        Assert.DoesNotContain("\noutput: csv:pwn.csv", composed.Yaml);
    }

    [Fact]
    public void ReadHeader_Stops_At_First_Root_Key_And_Ignores_Branch_Comments_Or_Later_Errors()
    {
        const string yaml = """
            # Title
            # Subtitle
            first:
              # Branch comment ignored in headerOnly mode
              input: csv:a.csv
            second: [unterminated
            """;

        var (header, branches) = JobYaml.ReadComments(yaml, headerOnly: true);

        Assert.Equal("Title\nSubtitle", header);
        Assert.Empty(branches);
        Assert.Equal("Title\nSubtitle", DesignService.ReadHeader(yaml));
    }

    [Fact]
    public void A_Branch_Comment_Is_Read_From_Its_Own_Line_Below_A_Scalar_Holding_A_Unicode_Line_Break()
    {
        // The scalar folds across the break, which the scanner counts as a line; a reader off by one line takes "Two" for "One".
        var yaml = "first:\n  input: \"csv:a\u2028    b.csv\"\nsecond:\n  # One\n  # Two\n  input: csv:c.csv\n";

        var (_, branches) = JobYaml.ReadComments(yaml);

        Assert.Equal("One\nTwo", branches["second"]);
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
