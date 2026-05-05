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
        _registry.Register(new FlagDef("--fake", new[] { "-f" }, FlagArity.Scalar, FlagScope.PerBranch));
        // Processor trigger flags (normally contributed by IStreamTransformerFactory.CliTriggerFlags)
        _registry.Register(new FlagDef("--sql", Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch));
        _registry.Register(new FlagDef("--merge", Array.Empty<string>(), FlagArity.Boolean, FlagScope.PerBranch));
        _lexer = new PipelineLexer(_registry);
    }

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

    [Fact]
    public void Parse_TwoIndependentBranches_ReturnsTwoBranches()
    {
        var args = new[] { "[", "-i", "a.csv", "-o", "out1.csv", "]", "[", "-i", "b.csv", "-o", "out2.csv", "]" };
        var pipeline = _lexer.Parse(args);

        Assert.Equal(2, pipeline.Branches.Count);
        Assert.Equal("a.csv", pipeline.Branches[0].Input);
        Assert.Equal("out1.csv", pipeline.Branches[0].Output);
        Assert.Equal("b.csv", pipeline.Branches[1].Input);
        Assert.Equal("out2.csv", pipeline.Branches[1].Output);
    }

    [Fact]
    public void Parse_FanOut_ReturnsCorrectTopology()
    {
        var args = new[] { "-i", "a.csv", "--alias", "s", "[", "--from", "s", "-o", "out1.csv", "]", "[", "--from", "s", "-o", "out2.csv", "]" };
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
        var args = new[] { "-i", "a.csv", "--alias", "src", "[", "--from", "src", "--sql", "SELECT * FROM src", "-o", "out.csv", "]" };
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
        var args = new[] { "-i", "a.csv", "--alias", "m", "[", "-i", "b.csv", "--alias", "r", "]", "[", "--from", "m", "--ref", "r", "--sql", "SELECT * FROM m JOIN r", "-o", "out.csv", "]" };
        var pipeline = _lexer.Parse(args);

        Assert.Equal(3, pipeline.Branches.Count);
        Assert.Equal("m", pipeline.Branches[2].From[0]);
        Assert.Equal("r", pipeline.Branches[2].Ref[0]);
        Assert.Contains("SELECT * FROM m JOIN r", pipeline.Branches[2].RawArgs);
    }

    [Fact]
    public void Parse_DryRun_HandledCorrectly()
    {
        // No value
        var pipeline1 = _lexer.Parse(new[] { "--dry-run", "-i", "gen:5" });
        Assert.Equal(1, pipeline1.Globals.DryRunCount);

        // With value
        var pipeline2 = _lexer.Parse(new[] { "--dry-run", "5", "-i", "gen:5" });
        Assert.Equal(5, pipeline2.Globals.DryRunCount);
        
        // Alias
        var pipeline3 = _lexer.Parse(new[] { "-dr", "10", "-i", "gen:5" });
        Assert.Equal(10, pipeline3.Globals.DryRunCount);
    }

    [Fact]
    public void Parse_GlobalsBeforeInput_CapturedInGlobals()
    {
        var args = new[] { "--batch-size", "1000", "--limit", "5000", "-i", "gen:10", "-o", "out.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Equal(1000, pipeline.Globals.BatchSize);
        Assert.Equal(5000, pipeline.Globals.Limit);
    }

    [Fact]
    public void Parse_Merge_HandledCorrectly()
    {
        var args = new[] { "-i", "a.csv", "--alias", "a", "[", "-i", "b.csv", "--alias", "b", "]", "[", "--from", "a,b", "--merge", "-o", "out.csv", "]" };
        var pipeline = _lexer.Parse(args);

        Assert.Equal(3, pipeline.Branches.Count);
        Assert.Contains("--merge", pipeline.Branches[2].RawArgs);
        Assert.Equal(new[] { "a", "b" }, pipeline.Branches[2].From);
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

    [Fact]
    public void Parse_LegacyTwoBranches_ImplicitlySplit()
    {
        var args = new[] { "-i", "a.csv", "-o", "out1.csv", "-i", "b.csv", "-o", "out2.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Equal(2, pipeline.Branches.Count);
        Assert.Equal("a.csv", pipeline.Branches[0].Input);
        Assert.Equal("b.csv", pipeline.Branches[1].Input);
    }

    [Fact]
    public void Parse_LegacyFanOut_ImplicitlySplit()
    {
        var args = new[] { "-i", "a.csv", "--alias", "s", "--from", "s", "-o", "out1.csv", "--from", "s", "-o", "out2.csv" };
        var pipeline = _lexer.Parse(args);

        Assert.Equal(3, pipeline.Branches.Count);
        Assert.Equal("a.csv", pipeline.Branches[0].Input);
        Assert.Equal("s", pipeline.Branches[1].From[0]);
        Assert.Equal("s", pipeline.Branches[2].From[0]);
    }
}
