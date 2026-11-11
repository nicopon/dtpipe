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
        _registry.Register(new FlagDef("--sql", Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch, "sql processor", FlagStage.Pipeline, ProcessorTrigger: true));
        _registry.Register(new FlagDef("--merge", Array.Empty<string>(), FlagArity.Boolean, FlagScope.PerBranch, "merge processor", FlagStage.Pipeline, ProcessorTrigger: true));
        // Registered at runtime from FilterOptions. Left out, it was an unknown flag here, which
        // now refuses the expression that follows it.
        _registry.Register(new FlagDef("--filter", Array.Empty<string>(), FlagArity.Repeatable, FlagScope.PerBranch, "filter transformer", FlagStage.Pipeline));

        // Contributed by every DB reader through QueryableReaderOptions (FlagStage.Reader).
        _registry.Register(new FlagDef("--query", new[] { "-q" }, FlagArity.Scalar, FlagScope.PerBranch, "query", FlagStage.Reader));

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
        Assert.Equal("a.csv", pipeline.Branches[0].Input);
        Assert.Equal("s", pipeline.Branches[0].Alias);
        Assert.Equal("s", pipeline.Branches[1].From[0]);
        Assert.Equal("out1.csv", pipeline.Branches[1].Output);
        Assert.Equal("s", pipeline.Branches[2].From[0]);
        Assert.Equal("out2.csv", pipeline.Branches[2].Output);
    }

    [Fact]
    public void Parse_SqlProcessor_ReturnsCorrectTopology()
    {
        var args = new[] { "-i", "a.csv", "--alias", "src", "--from", "src", "--sql", "SELECT * FROM src", "-o", "out.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Equal(2, pipeline.Branches.Count);
        Assert.Equal("a.csv", pipeline.Branches[0].Input);
        Assert.Equal("src", pipeline.Branches[0].Alias);

        Assert.Equal("src", pipeline.Branches[1].From[0]);
        Assert.Contains("--sql", pipeline.Branches[1].RawArgs);
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
        Assert.Equal("m", pipeline.Branches[2].From[0]);
        Assert.Equal("r", pipeline.Branches[2].Ref[0]);
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
    /// --session is scalar like every other value flag. Only -i and --from give repetition
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
    /// Neither materialisation flag opens a branch. Only -i and --from do, and giving a
    /// third flag that meaning would make the grammar unlearnable.
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

    /// <summary>A retired switch is refused by name, with or without --strict-bindings: tolerated as
    /// an unknown boolean, the run would go on without what the command line asks for.</summary>
    [Fact]
    public void A_Retired_Flag_Is_Refused_By_Name()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => _lexer.Parse(new[] { "-i", "a.csv", "-o", "out.csv", "--retry" }));

        Assert.Contains("--retry", ex.Message);
        Assert.Contains("production.md", ex.Message);
    }

    // ── Reader flags in a branch that has no reader ─────────────────────

    [Theory]
    [InlineData("--query")]
    [InlineData("-q")]
    [InlineData("--table")]
    public void Parse_ReaderFlagOnAFromBranch_Throws(string flag)
    {
        var args = new[] { "-i", "a.csv", "--alias", "A", "-i", "b.csv", "--alias", "B",
                           "--from", "A", "--ref", "B", flag, "SELECT 1", "-o", "out.csv" };

        var ex = Assert.Throws<InvalidOperationException>(() => _lexer.Parse(args));
        Assert.Contains(flag, ex.Message);
        Assert.Contains("no reader of its own", ex.Message);
    }

    [Fact]
    public void Parse_ReaderOnlyFlagOnAFromBranch_ListsTheProcessorsTheRegistryCarries()
    {
        var args = new[] { "-i", "a.csv", "--alias", "A", "--from", "A", "--query", "SELECT 1", "-o", "out.csv" };

        var ex = Assert.Throws<InvalidOperationException>(() => _lexer.Parse(args));

        Assert.Contains("--sql <value>", ex.Message);   // scalar trigger
        Assert.Contains("--merge", ex.Message);         // boolean trigger, no value marker
        Assert.DoesNotContain("--filter", ex.Message);  // a transformer is not a processor
    }

    /// <summary>
    /// The catalogue is read, not restated: a registry carrying different processors produces a
    /// different list, so adding, renaming or retiring one cannot leave the message behind.
    /// The trigger is named so no real processor can ever claim it — a plausible name like
    /// '--pivot' would quietly stop testing genericity the day the catalogue grew one.
    /// </summary>
    [Fact]
    public void Parse_ProcessorHint_FollowsTheRegistryRatherThanAFixedList()
    {
        var registry = new FlagRegistry();
        CoreFlagRegistry.RegisterCoreFlags(registry);
        registry.Register(new FlagDef("--query", new[] { "-q" }, FlagArity.Scalar, FlagScope.PerBranch, "query", FlagStage.Reader));
        registry.Register(new FlagDef("--unknown-processor", Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch, "unknown-processor", FlagStage.Pipeline, ProcessorTrigger: true));

        var ex = Assert.Throws<InvalidOperationException>(() => new PipelineLexer(registry).Parse(
            new[] { "-i", "a.csv", "--alias", "A", "--from", "A", "--query", "SELECT 1", "-o", "out.csv" }));

        Assert.Contains("--unknown-processor <value>", ex.Message);
        Assert.DoesNotContain("--sql", ex.Message);
    }

    /// <summary>A flag that also configures a writer is misplaced, not unbindable — say so.</summary>
    [Fact]
    public void Parse_ReaderAndWriterFlagOnAFromBranch_SaysToMoveItAfterOutput()
    {
        var args = new[] { "-i", "a.csv", "--alias", "A", "--from", "A", "--table", "t", "-o", "out.csv" };

        var ex = Assert.Throws<InvalidOperationException>(() => _lexer.Parse(args));

        Assert.Contains("move it after -o", ex.Message);
        Assert.DoesNotContain("stream processor", ex.Message);
    }

    [Fact]
    public void Parse_EngineFlagsOnAFromBranch_AreAccepted()
    {
        var args = new[] { "-i", "a.csv", "--alias", "A", "--from", "A",
                           "--batch-size", "1024", "--limit", "10", "-o", "out.csv" };

        var pipeline = _lexer.Parse(args);

        var consumer = pipeline.Branches.Last();
        Assert.Equal(new[] { "A" }, consumer.From);
        Assert.Equal("out.csv", consumer.Output);
    }

    /// <summary>
    /// A job file supplies the branches, so a reader flag on a branch of the line is no override:
    /// the line is refused at its first branch flag.
    /// </summary>
    [Fact]
    public void Parse_ReaderFlagOnAFromBranch_IsRefusedWithAJobFile()
    {
        var args = new[] { "--job", "j.yaml", "--from", "A", "--query", "SELECT 1", "-o", "out.csv" };

        var ex = Assert.Throws<InvalidOperationException>(() => _lexer.Parse(args));

        Assert.Contains("Flag '--from' cannot be combined with --job", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_ReaderFlagOnABranchThatHasAReader_IsAccepted()
    {
        var args = new[] { "-i", "db:conn", "--query", "SELECT 1", "-o", "out.csv" };

        var pipeline = _lexer.Parse(args);

        Assert.Single(pipeline.Branches);
        Assert.Contains("--query", pipeline.Branches[0].ReaderArgs);
    }

    // ── A flag given twice in one branch ────────────────────────────────

    /// <summary>
    /// A flag of the whole branch (FlagStage.All) is refused the second time it is given, in the
    /// same stage or in another, and the refusal names both values: kept last, the first one would
    /// be dropped without a word, since -o does not open a branch. A flag the reader and the writer
    /// each bind (FlagStage.Any) still takes one value in each stage.
    /// </summary>
    [Theory]
    [InlineData(new[] { "-i", "shop.db", "-t", "orders", "--alias", "a", "--alias", "b", "-o", "x.csv" }, "--alias", "a", "b")]
    [InlineData(new[] { "-i", "shop.db", "-t", "orders", "--alias", "orders", "-o", "x.csv", "--alias", "report" }, "--alias", "orders", "report")]
    [InlineData(new[] { "-i", "shop.db", "-t", "orders", "--alias", "A", "-o", "x.csv", "--alias", "B", "--export-job", "e.yaml" }, "--alias", "A", "B")]
    [InlineData(new[] { "-i", "shop.db", "-t", "orders", "--limit", "3", "-o", "x.csv", "--limit", "5" }, "--limit", "3", "5")]
    [InlineData(new[] { "-i", "shop.db", "-t", "orders", "-o", "copy.db", "-t", "copy" }, null, null, null)]
    public void A_Branch_Flag_Is_Given_Once_Per_Branch_A_Stage_Flag_Once_Per_Stage(
        string[] args, string? flag, string? first, string? second)
    {
        if (flag is null)
        {
            var branch = Assert.Single(_lexer.Parse(args).Branches);
            Assert.Equal(new[] { "-i", "shop.db", "-t", "orders" }, branch.ReaderArgs);
            Assert.Equal(new[] { "-o", "copy.db", "-t", "copy" }, branch.WriterArgs);
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() => _lexer.Parse(args));
        Assert.Contains($"'{flag}' is given twice in this branch: '{first}' and '{second}'", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A command line runs one job file, and -j is the same flag as --job. A second one would
    /// replace the first in silence, so the refusal names both files.
    /// </summary>
    [Theory]
    [InlineData(new[] { "--job", "ja.yaml", "--job", "jb.yaml", "--no-stats" }, "ja.yaml", "jb.yaml")]
    [InlineData(new[] { "--job", "ja.yaml", "-j", "jb.yaml" }, "ja.yaml", "jb.yaml")]
    [InlineData(new[] { "-j", "ja.yaml", "--job", "jb.yaml" }, "ja.yaml", "jb.yaml")]
    public void A_Second_Job_File_Is_Refused_Naming_Both_Files(string[] args, string first, string second)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => _lexer.Parse(args));

        Assert.Contains($"'--job' is given twice: '{first}' and '{second}'", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A job file is the source of the branches: a flag of a branch given beside it would be lexed,
    /// then dropped with exit 0, so it is refused, named as typed (<c>-o</c>, not <c>--output</c>).
    /// The engine controls the conversion applies to the loaded job stay allowed, spelled by name or
    /// by alias, with the flags of the whole command line; one the conversion does not read
    /// (<c>--checkpoint</c>) is refused like a branch flag.
    /// </summary>
    [Theory]
    [InlineData(new[] { "--job", "ja.yaml", "-i", "generate:2", "-o", "x.csv" }, "-i")]
    [InlineData(new[] { "-i", "generate:2", "-o", "x.csv", "--job", "ja.yaml" }, "-i")]
    [InlineData(new[] { "--job", "ja.yaml", "--from", "s", "-o", "x.csv" }, "--from")]
    [InlineData(new[] { "--job", "ja.yaml", "-o", "x.csv" }, "-o")]
    [InlineData(new[] { "--job", "ja.yaml", "--output", "x.csv" }, "--output")]
    [InlineData(new[] { "-j", "ja.yaml", "--alias", "z" }, "--alias")]
    [InlineData(new[] { "--job", "ja.yaml", "--fake", "GenerateIndex:name.fullName" }, "--fake")]
    [InlineData(new[] { "--job", "ja.yaml", "--filter", "GenerateIndex > 0" }, "--filter")]
    [InlineData(new[] { "--job", "ja.yaml", "--sql", "SELECT 1" }, "--sql")]
    [InlineData(new[] { "--job", "ja.yaml", "--limit", "2", "-o", "x.csv" }, "-o")]
    [InlineData(new[] { "--job", "ja.yaml", "--checkpoint", "stage1" }, "--checkpoint")]
    [InlineData(new[] { "--job", "ja.yaml", "--limit", "2", "-b", "10", "--prefix", "p_", "--sample-rate", "0.5", "--sampling-seed", "-5", "--dry-run", "1", "--no-stats" }, null)]
    [InlineData(new[] { "--job", "ja.yaml", "-p", "p_", "--cursor", "Id", "--state", "s.json", "--max-batch-bytes", "1", "--session", "s", "--metrics-path", "m.json" }, null)]
    [InlineData(new[] { "--job", "ja.yaml", "--bind-output", "a=-", "--bind-compression", "zstd", "--bind-compression-level", "3" }, null)]
    [InlineData(new[] { "--job", "ja.yaml", "--edges-only", "--bind-output", "a=pipe://p" }, null)]
    public void A_Branch_Flag_Beside_A_Job_File_Is_Refused_Naming_The_Flag(string[] args, string? refused)
    {
        if (refused is null)
        {
            Assert.Equal("ja.yaml", _lexer.Parse(args).Globals.JobFile);
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() => _lexer.Parse(args));
        Assert.Contains($"Flag '{refused}' cannot be combined with --job", ex.Message, StringComparison.Ordinal);
        Assert.Contains("'ja.yaml' is the source of the branches", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_Bind_Compression_Flags_Reach_The_Globals_Beside_A_Job_File()
    {
        var globals = _lexer.Parse(new[] { "--job", "ja.yaml", "--bind-compression", "lz4", "--bind-compression-level", "4" }).Globals;

        Assert.Equal("lz4", globals.BindCompression);
        Assert.Equal("4", globals.BindCompressionLevel);
    }

    [Fact]
    public void The_Edges_Only_Flag_Reaches_The_Globals_Beside_A_Job_File()
    {
        Assert.True(_lexer.Parse(new[] { "--job", "ja.yaml", "--edges-only" }).Globals.EdgesOnly);
        Assert.False(_lexer.Parse(new[] { "--job", "ja.yaml" }).Globals.EdgesOnly);
    }

    [Theory]
    [InlineData("--bind-compression", "lz4", "zstd")]
    [InlineData("--bind-compression-level", "1", "2")]
    public void A_Repeated_Bind_Compression_Flag_Is_Refused(string flag, string first, string second)
    {
        var args = new[] { "-i", "generate:2", "-o", "x.csv", flag, first, flag, second };

        Assert.Throws<InvalidOperationException>(() => _lexer.Parse(args));
    }

    [Fact]
    public void Export_Job_Without_File_At_End_Of_Command_Line_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _lexer.Parse(new[] { "-i", "in.csv", "-o", "out.csv", "--export-job" }));

        Assert.Contains("'--export-job' requires a file path", ex.Message, StringComparison.Ordinal);
        Assert.Contains("the target file name is mandatory", ex.Message, StringComparison.Ordinal);
        Assert.Contains("nothing follows it", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Export_Job_Alone_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _lexer.Parse(new[] { "--export-job" }));

        Assert.Contains("'--export-job' requires a file path", ex.Message, StringComparison.Ordinal);
        Assert.Contains("the target file name is mandatory", ex.Message, StringComparison.Ordinal);
        Assert.Contains("nothing follows it", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("-o")]
    [InlineData("--no-stats")]
    [InlineData("--dry-run")]
    public void Export_Job_Followed_By_Flag_Throws(string nextFlag)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _lexer.Parse(new[] { "-i", "in.csv", "--export-job", nextFlag, "-o", "out.csv" }));

        Assert.Contains("'--export-job' requires a file path", ex.Message, StringComparison.Ordinal);
        Assert.Contains("the target file name is mandatory", ex.Message, StringComparison.Ordinal);
        Assert.Contains(nextFlag, ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Export_Job_With_Whitespace_Or_Empty_Target_Throws(string emptyTarget)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _lexer.Parse(new[] { "-i", "in.csv", "-o", "out.csv", "--export-job", emptyTarget }));

        Assert.Contains("'--export-job' requires a file path", ex.Message, StringComparison.Ordinal);
        Assert.Contains("the target file name is mandatory", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Export_Job_With_Valid_File_Sets_ExportJobFile()
    {
        var pipeline = _lexer.Parse(new[] { "-i", "in.csv", "-o", "out.csv", "--export-job", "exported.yaml" });

        Assert.Equal("exported.yaml", pipeline.Globals.ExportJobFile);
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
