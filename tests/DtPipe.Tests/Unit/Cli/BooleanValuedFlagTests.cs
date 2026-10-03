using System;
using System.Linq;
using DtPipe.Adapters.Csv;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// An option that is on by default takes <c>true|false</c>: written alone it would change nothing,
/// and without a value nothing could turn it off. The lexer must refuse every spelling that
/// used to run with the option unchanged.
/// </summary>
public class BooleanValuedFlagTests
{
    private readonly PipelineLexer _lexer;

    public BooleanValuedFlagTests()
    {
        var registry = new FlagRegistry();
        CoreFlagRegistry.RegisterCoreFlags(registry);
        foreach (var def in CliOptionBuilder.GenerateFlagDefsForType(typeof(CsvReaderOptions)))
            registry.Register(def with { Stage = FlagStage.Reader });
        foreach (var def in CliOptionBuilder.GenerateFlagDefsForType(typeof(CsvWriterOptions)))
            registry.Register(def with { Stage = FlagStage.Writer });
        _lexer = new PipelineLexer(registry);
    }

    [Fact]
    public void A_Default_True_Option_Is_Published_With_A_Value()
    {
        var header = CliOptionBuilder.GenerateFlagDefsForType(typeof(CsvWriterOptions)).Single(d => d.Name == "--csv-header");

        Assert.Equal(FlagArity.Scalar, header.Arity);
        Assert.True(header.BooleanValued);
    }

    [Theory]
    [InlineData("--csv-header")]
    [InlineData("--csv-has-header")]
    public void The_Value_Reaches_The_Branch(string flag)
    {
        var reading = flag == "--csv-has-header";
        var args = reading
            ? new[] { "-i", "csv:in.csv", flag, "false", "-o", "csv:out.csv" }
            : new[] { "-i", "csv:in.csv", "-o", "csv:out.csv", flag, "false" };

        var branch = _lexer.Parse(args).Branches.Single();

        Assert.Equal("false", branch.Flags[flag].Single());
    }

    [Fact]
    public void The_Flag_Alone_Before_Another_Flag_Is_Refused_Rather_Than_Swallowing_It()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _lexer.Parse(new[] { "-i", "csv:in.csv", "--csv-has-header", "-o", "csv:out.csv" }));

        Assert.Contains("'--csv-has-header' takes true or false, not '-o'", ex.Message);
    }

    [Fact]
    public void The_Flag_Alone_At_The_End_Is_Refused()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _lexer.Parse(new[] { "-i", "csv:in.csv", "-o", "csv:out.csv", "--csv-header" }));

        Assert.Contains("nothing follows it", ex.Message);
    }

    [Fact]
    public void Something_Other_Than_True_Or_False_Is_Refused_At_The_Flag()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _lexer.Parse(new[] { "-i", "csv:in.csv", "-o", "csv:out.csv", "--csv-header", "no" }));

        Assert.Contains("'--csv-header' takes true or false, not 'no'", ex.Message);
    }

    [Fact]
    public void The_Equals_Spelling_Is_Refused_And_Names_The_Spelling_That_Works()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _lexer.Parse(new[] { "-i", "csv:in.csv", "-o", "csv:out.csv", "--csv-header=false" }));

        Assert.Contains("--csv-header false", ex.Message);
    }

    [Theory]
    [InlineData("false")]
    [InlineData("True")]
    public void A_Value_After_A_Switch_Is_Refused_Rather_Than_Read_As_A_Query(string literal)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _lexer.Parse(new[] { "-i", "csv:in.csv", "--no-stats", literal, "-o", "csv:out.csv" }));

        Assert.Contains($"'--no-stats' takes no value, so '{literal}' cannot follow it", ex.Message);
    }

    [Fact]
    public void A_Query_After_A_Switch_Is_Still_A_Query()
    {
        var pipeline = _lexer.Parse(new[] { "-i", "csv:in.csv", "--no-stats", "SELECT 1", "-o", "csv:out.csv" });

        Assert.Equal("SELECT 1", pipeline.Branches.Last().Flags["--sql"].Single());
    }
}
