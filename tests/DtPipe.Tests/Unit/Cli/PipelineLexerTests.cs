using DtPipe.Cli.Pipeline;
using Xunit;
using System.Linq;

namespace DtPipe.Tests.Unit.Cli;

public class PipelineLexerTests
{
    private readonly FlagRegistry _registry;
    private readonly PipelineLexer _lexer;

    public PipelineLexerTests()
    {
        _registry = new FlagRegistry();
        CoreFlagRegistry.RegisterCoreFlags(_registry);

        // Engine controls (FlagStage.All — valid in any position)
        foreach (var def in new DtPipe.Cli.Infrastructure.PipelineOptionsCliContributor().GetFlagDefs())
            _registry.Register(def with { Stage = FlagStage.All });

        // Reader-specific flags (FlagStage.Reader)
        _registry.Register(new FlagDef("--fake", new[] { "-f" }, FlagArity.Scalar, FlagScope.PerBranch, "fake transformer", FlagStage.Pipeline));
        _registry.Register(new FlagDef("--sql",  Array.Empty<string>(), FlagArity.Scalar,  FlagScope.PerBranch, "sql processor",   FlagStage.Pipeline, ProcessorTrigger: true));
        _registry.Register(new FlagDef("--merge", Array.Empty<string>(), FlagArity.Boolean, FlagScope.PerBranch, "merge processor", FlagStage.Pipeline, ProcessorTrigger: true));
        // Registered at runtime from FilterOptions. Left out, it was an unknown flag here, which
        // now refuses the expression that follows it.
        _registry.Register(new FlagDef("--filter", Array.Empty<string>(), FlagArity.Repeatable, FlagScope.PerBranch, "filter transformer", FlagStage.Pipeline));

        // Shared reader+writer flags (FlagStage.Any = Reader | Writer)
        _registry.Register(new FlagDef("--table", new[] { "-t" }, FlagArity.Scalar, FlagScope.PerBranch, "table", FlagStage.Any));
        _registry.Register(new FlagDef("--strategy", new[] { "-s" }, FlagArity.Scalar, FlagScope.PerBranch, "strategy", FlagStage.Any));

        _lexer = new PipelineLexer(_registry);
    }

    // ── Linear pipelines ───────────────────────────────────────────────

    [Fact]
    public void Parse_LinearSimple_ReturnsOneBranch()
    {
        var args = new[] { "-i", "gen:10", "-o", "out.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Single(pipeline.Branches);
        Assert.Equal("gen:10", pipeline.Branches[0].Input);
        Assert.Equal("out.csv", pipeline.Branches[0].Output);
    }

    [Fact]
    public void Parse_WithTransformers_ReturnsCorrectBranch()
    {
        var args = new[] { "-i", "gen:10", "--fake", "Id:random.uuid", "-o", "out.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Single(pipeline.Branches);
        Assert.Equal("gen:10", pipeline.Branches[0].Input);
        Assert.Equal("out.csv", pipeline.Branches[0].Output);
        Assert.Contains("--fake", pipeline.Branches[0].RawArgs);
        Assert.Contains("Id:random.uuid", pipeline.Branches[0].RawArgs);
    }

    // ── Stage splitting ────────────────────────────────────────────────

    [Fact]
    public void Parse_TransformerFlagSplitsStages()
    {
        var args = new[] { "-i", "gen:10", "--fake", "Id:random.uuid", "-o", "out.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Single(pipeline.Branches);
        var branch = pipeline.Branches[0];

        // Reader scope: everything before --fake
        Assert.Contains("-i", branch.ReaderArgs);
        Assert.Contains("gen:10", branch.ReaderArgs);
        Assert.DoesNotContain("--fake", branch.ReaderArgs);

        // Pipeline scope: transformer and its value
        Assert.Contains("--fake", branch.PipelineArgs);
        Assert.Contains("Id:random.uuid", branch.PipelineArgs);
        Assert.DoesNotContain("-o", branch.PipelineArgs);

        // Writer scope: from -o to end
        Assert.Contains("-o", branch.WriterArgs);
        Assert.Contains("out.csv", branch.WriterArgs);
        Assert.DoesNotContain("--fake", branch.WriterArgs);
    }

    [Fact]
    public void Parse_NoTransformer_AllArgsSplitBetweenReaderAndWriter()
    {
        var args = new[] { "-i", "gen:5", "--table", "src", "-o", "out.csv", "--table", "tgt", "--strategy", "Recreate" };
        var pipeline = _lexer.Parse(args);

        var branch = pipeline.Branches[0];
        Assert.Contains("--table", branch.ReaderArgs);   // --table before -o → reader scope
        Assert.Contains("src", branch.ReaderArgs);
        Assert.Empty(branch.PipelineArgs);
        Assert.Contains("--table", branch.WriterArgs);   // --table after -o → writer scope
        Assert.Contains("tgt", branch.WriterArgs);
        Assert.Contains("--strategy", branch.WriterArgs);
    }

    [Fact]
    public void Parse_GlobalBatchSize_CapturedInGlobals()
    {
        var args = new[] { "--batch-size", "1000", "--limit", "5000", "-i", "gen:10", "-o", "out.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Equal("1000", pipeline.Globals.AllFlags["--batch-size"]?.ToString());
        Assert.Equal("5000", pipeline.Globals.AllFlags["--limit"]?.ToString());
    }

    // ── Stage validation (strict) ──────────────────────────────────────

    [Fact]
    public void Parse_WriterFlagInPipelineScope_Throws()
    {
        // --table is FlagStage.Any (Reader|Writer), not Pipeline → error after --fake
        var args = new[] { "-i", "gen:5", "--fake", "col:random.uuid", "--table", "wrong", "-o", "out.csv" };
        Assert.Throws<InvalidOperationException>(() => _lexer.Parse(args));
    }

    [Fact]
    public void Parse_TransformerFlagInWriterScope_Throws()
    {
        // --fake is FlagStage.Pipeline → error after -o
        var args = new[] { "-i", "gen:5", "-o", "out.csv", "--fake", "col:random.uuid" };
        Assert.Throws<InvalidOperationException>(() => _lexer.Parse(args));
    }

    // ── Implicit branch splitting ──────────────────────────────────────

    [Fact]
    public void Parse_TwoInputs_ReturnsTwoBranches()
    {
        var args = new[] { "-i", "a.csv", "-o", "out1.csv", "-i", "b.csv", "-o", "out2.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Equal(2, pipeline.Branches.Count);
        Assert.Equal("a.csv", pipeline.Branches[0].Input);
        Assert.Equal("b.csv", pipeline.Branches[1].Input);
    }

    [Fact]
    public void Parse_FanOut_ReturnsThreeBranches()
    {
        // Source branch (1), two consumer branches split by --from
        var args = new[] { "-i", "a.csv", "--alias", "s", "--from", "s", "-o", "out1.csv", "--from", "s", "-o", "out2.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Equal(3, pipeline.Branches.Count);
        Assert.Equal("a.csv",   pipeline.Branches[0].Input);
        Assert.Equal("s",       pipeline.Branches[0].Alias);
        Assert.Equal("s",       pipeline.Branches[1].From[0]);
        Assert.Equal("out1.csv",pipeline.Branches[1].Output);
        Assert.Equal("s",       pipeline.Branches[2].From[0]);
        Assert.Equal("out2.csv",pipeline.Branches[2].Output);
    }

    [Fact]
    public void Parse_SqlProcessor_ReturnsCorrectTopology()
    {
        var args = new[] { "-i", "a.csv", "--alias", "src", "--from", "src", "--sql", "SELECT * FROM src", "-o", "out.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Equal(2, pipeline.Branches.Count);
        Assert.Equal("a.csv", pipeline.Branches[0].Input);
        Assert.Equal("src",   pipeline.Branches[0].Alias);

        Assert.Equal("src", pipeline.Branches[1].From[0]);
        Assert.Contains("--sql",            pipeline.Branches[1].RawArgs);
        Assert.Contains("SELECT * FROM src", pipeline.Branches[1].RawArgs);
        Assert.Equal("out.csv", pipeline.Branches[1].Output);
    }

    [Fact]
    public void Parse_SqlWithRef_ReturnsCorrectTopology()
    {
        var args = new[] {
            "-i", "a.csv", "--alias", "m",
            "-i", "b.csv", "--alias", "r",
            "--from", "m", "--ref", "r", "--sql", "SELECT * FROM m JOIN r", "-o", "out.csv"
        };
        var pipeline = _lexer.Parse(args);

        Assert.Equal(3, pipeline.Branches.Count);
        Assert.Equal("m",   pipeline.Branches[2].From[0]);
        Assert.Equal("r",   pipeline.Branches[2].Ref[0]);
        Assert.Contains("SELECT * FROM m JOIN r", pipeline.Branches[2].RawArgs);
    }

    private string[] SqlBranchWith(params string[] refArgs) => new[] {
        "-i", "a.csv", "--alias", "m",
        "-i", "b.csv", "--alias", "r",
        "-i", "c.csv", "--alias", "r2",
        "--from", "m"
    }.Concat(refArgs).Concat(new[] {
        "--sql", "SELECT * FROM m JOIN r JOIN r2", "-o", "out.csv"
    }).ToArray();

    [Fact]
    public void Parse_CommaSeparatedRefs_AreOneAliasList()
    {
        var pipeline = _lexer.Parse(SqlBranchWith("--ref", "r,r2"));

        Assert.Equal(new[] { "r", "r2" }, pipeline.Branches[^1].Ref);
    }

    /// <summary>
    /// Repetition means "open a branch" everywhere in this grammar. A value flag that accumulated
    /// instead would teach the same spelling for '--from', where it starts a second branch.
    /// </summary>
    [Fact]
    public void Parse_RepeatedRef_ThrowsAndNamesTheCommaForm()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => _lexer.Parse(SqlBranchWith("--ref", "r", "--ref", "r2")));

        Assert.Contains("--ref a,b", ex.Message);
    }

    [Fact]
    public void Parse_Session_IsGlobalAndDoesNotSplitTheBranch()
    {
        var pipeline = _lexer.Parse(new[] { "-i", "in.csv", "--session", "mission-7", "-o", "out.csv" });

        Assert.Single(pipeline.Branches);
        Assert.Equal("mission-7", pipeline.Globals.Session);
    }

    /// <summary>
    /// --session is scalar like every other value flag. Only -i, --from and --job give repetition
    /// a meaning, and that meaning is "open a branch".
    /// </summary>
    [Fact]
    public void Parse_RepeatedSession_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => _lexer.Parse(new[] { "-i", "in.csv", "--session", "a", "--session", "b", "-o", "out.csv" }));

        Assert.Contains("--session", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Neither materialisation flag opens a branch. Only -i, --from and --job do, and giving a
    /// fourth flag that meaning would make the grammar unlearnable.
    /// </summary>
    [Fact]
    public void Parse_CheckpointFlags_DoNotSplitTheBranch()
    {
        var pipeline = _lexer.Parse(new[]
        {
            "-i", "in.csv", "--checkpoint", "stage1", "--from-checkpoint", "abc123", "-o", "out.csv"
        });

        Assert.Single(pipeline.Branches);
    }

    [Fact]
    public void Parse_RepeatedFromWithEmptyBranch_Throws()
    {
        var args = new[] {
            "-i", "a.csv", "--alias", "a",
            "-i", "b.csv", "--alias", "b",
            "--from", "a", "--from", "b", "--merge", "-o", "out.csv"
        };

        var ex = Assert.Throws<InvalidOperationException>(() => _lexer.Parse(args));
        Assert.Contains("--from a,", ex.Message);
    }

    [Fact]
    public void Parse_RepeatedFromWithPopulatedBranches_IsAccepted()
    {
        // Diamond and fan-out both repeat --from; each branch carries an alias or an output.
        var args = new[] {
            "-i", "s.csv", "--alias", "s",
            "--from", "s", "--filter", "row.x > 1", "--alias", "hi",
            "--from", "s", "--filter", "row.x <= 1", "--alias", "lo",
            "--from", "hi", "-o", "a.csv",
            "--from", "lo", "-o", "b.csv"
        };

        var pipeline = _lexer.Parse(args);

        Assert.Equal(5, pipeline.Branches.Count);
    }

    [Fact]
    public void Parse_Merge_ReturnsCorrectTopology()
    {
        var args = new[] {
            "-i", "a.csv", "--alias", "a",
            "-i", "b.csv", "--alias", "b",
            "--from", "a,b", "--merge", "-o", "out.csv"
        };
        var pipeline = _lexer.Parse(args);

        Assert.Equal(3, pipeline.Branches.Count);
        Assert.Contains("--merge", pipeline.Branches[2].RawArgs);
        Assert.Equal(new[] { "a", "b" }, pipeline.Branches[2].From.ToArray());
    }

    // ── Dry-run and global flags ───────────────────────────────────────

    [Fact]
    public void Parse_DryRun_HandledCorrectly()
    {
        var pipeline1 = _lexer.Parse(new[] { "--dry-run", "-i", "gen:5" });
        Assert.Equal(1, pipeline1.Globals.DryRunCount);

        var pipeline2 = _lexer.Parse(new[] { "--dry-run", "5", "-i", "gen:5" });
        Assert.Equal(5, pipeline2.Globals.DryRunCount);

        var pipeline3 = _lexer.Parse(new[] { "-dr", "10", "-i", "gen:5" });
        Assert.Equal(10, pipeline3.Globals.DryRunCount);
    }

    [Fact]
    public void Parse_UnknownFlag_StoredAsBoolean()
    {
        var args = new[] { "-i", "gen:10", "--custom-flag", "-o", "out.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Contains("--custom-flag", pipeline.Branches[0].RawArgs);
    }

    [Fact]
    public void Parse_PositionalSql_HandledCorrectly()
    {
        var args = new[] { "-i", "a.csv", "--alias", "s", "--from", "s", "SELECT * FROM s", "-o", "out.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Contains("SELECT * FROM s", pipeline.Branches[1].RawArgs);
    }

    /// <summary>
    /// An unrecognised flag is kept as a boolean for OptionBinder, so it does not consume the token
    /// after it — which then reached the positional-query rule. A misspelt '--ouput out.csv' built a
    /// DuckDB branch and died on "Parser Error: syntax error at or near 'out.csv'", never naming the
    /// flag. Refusing here is the only place that still knows the two tokens were adjacent.
    /// </summary>
    [Fact]
    public void A_Positional_After_An_Unknown_Flag_Names_The_Flag()
    {
        var args = new[] { "-i", "a.csv", "--ouput", "out.csv" };

        var ex = Assert.Throws<InvalidOperationException>(() => _lexer.Parse(args));

        Assert.Contains("--ouput", ex.Message);
        Assert.Contains("out.csv", ex.Message);
    }

    /// <summary>The refusal is about adjacency: an unknown flag followed by a recognised one stays
    /// the boolean it has always been.</summary>
    [Fact]
    public void An_Unknown_Flag_Followed_By_A_Known_One_Is_Still_A_Boolean()
    {
        var pipeline = _lexer.Parse(new[] { "-i", "a.csv", "--custom-flag", "-o", "out.csv" });

        Assert.Contains("--custom-flag", pipeline.Branches[0].RawArgs);
    }
}

[Collection("console-serial")]
public class RepeatedFlagStrictnessTests
{
    private static PipelineLexer BuildLexer()
    {
        var registry = new FlagRegistry();
        CoreFlagRegistry.RegisterCoreFlags(registry);
        // Mirror runtime registration of the CSV flags used by these tests. At runtime the
        // reader AND writer contributors both register --csv-separator, merging Stage to
        // Reader|Writer (FlagStage.Any) — cross-stage repeats are then legitimate.
        foreach (var def in DtPipe.Cli.Infrastructure.CliOptionBuilder.GenerateFlagDefsForType(typeof(DtPipe.Adapters.Csv.CsvReaderOptions)))
            registry.Register(def with { Stage = FlagStage.Any });
        // --sql comes from the stream-processor trigger flags at runtime, not from the core set.
        // Leaving it unregistered made it an unknown flag here, which now refuses the query that
        // follows it — these tests are about --sql's own duplicate rule, so it must be known.
        registry.Register(new FlagDef("--sql", Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch, "sql processor", FlagStage.Pipeline, ProcessorTrigger: true));
        return new PipelineLexer(registry);
    }

    private static string ParseCapturingStderr(params string[] args)
    {
        var lexer = BuildLexer();

        var originalError = Console.Error;
        var captured = new StringWriter();
        Console.SetError(captured);
        try
        {
            lexer.Parse(args);
        }
        finally
        {
            Console.SetError(originalError);
        }
        return captured.ToString();
    }

    [Fact]
    public void Repeated_Scalar_Flag_In_Same_Stage_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ParseCapturingStderr("-i", "in.csv", "--csv-separator", ";", "--csv-separator", "|", "-o", "out.csv"));
        Assert.Contains("more than once", ex.Message, StringComparison.Ordinal);
        Assert.Contains("--csv-separator", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Cross_Stage_Repeat_Is_Allowed()
    {
        // Reader separator and writer separator are independent bindings in one branch.
        var pipeline = BuildLexer().Parse(new[] { "-i", "in.csv", "--csv-separator", ";", "-o", "out.csv", "--csv-separator", "|" });
        Assert.Single(pipeline.Branches);
    }

    [Fact]
    public void Repeated_Global_Flag_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ParseCapturingStderr("-i", "in.csv", "--log", "a.log", "--log", "b.log", "-o", "out.csv"));
        Assert.Contains("--log", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Positional_Query_After_Explicit_Sql_In_From_Branch_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ParseCapturingStderr("-i", "in.csv", "--alias", "s", "--from", "s", "--sql", "SELECT 1", "SELECT 2"));
        Assert.Contains("SQL query provided more than once", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// This ordering is refused by the generic duplicate-flag rule rather than the dedicated
    /// message: a positional query registers itself as '--sql', so the explicit one that follows is
    /// a second occurrence in the same stage. Both refuse; only the wording differs.
    /// </summary>
    [Fact]
    public void Explicit_Sql_After_Positional_Query_In_From_Branch_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ParseCapturingStderr("-i", "in.csv", "--alias", "s", "--from", "s", "SELECT 1", "--sql", "SELECT 2"));
        Assert.Contains("appears more than once in the same branch stage", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Distinct_Flags_Do_Not_Warn()
    {
        var output = ParseCapturingStderr("-i", "in.csv", "-o", "out.csv", "--no-stats");
        Assert.DoesNotContain("Warning: flag", output, StringComparison.Ordinal);
    }

    [Fact]
    public void Transformer_Options_May_Repeat_Across_Instances()
    {
        // Multi-instance idiom: every --fake starts a new transformer instance whose
        // options (--fake-seed-row) scope to that instance — repetition is legitimate.
        var registry = new FlagRegistry();
        CoreFlagRegistry.RegisterCoreFlags(registry);
        registry.Register(new FlagDef("--fake", Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch, "fake transformer", FlagStage.Pipeline));
        registry.Register(new FlagDef("--fake-seed-row", Array.Empty<string>(), FlagArity.Boolean, FlagScope.PerBranch, "seed per row", FlagStage.Pipeline));
        registry.Register(new FlagDef("--drop", Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch, "drop column", FlagStage.Pipeline));

        var pipeline = new PipelineLexer(registry).Parse(new[]
        {
            "-i", "generate:20",
            "--fake", "Id:random.number", "--fake-seed-row",
            "--fake", "Name:name.fullName", "--fake-seed-row",
            "--drop", "GenerateIndex",
            "-o", "out.csv"
        });

        Assert.Single(pipeline.Branches);
    }
}
