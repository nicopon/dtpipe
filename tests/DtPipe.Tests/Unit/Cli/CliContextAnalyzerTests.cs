using System.CommandLine;
using DtPipe.Cli;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

public class CliContextAnalyzerTests
{
    private readonly IReadOnlyList<Option> _options;

    public CliContextAnalyzerTests()
    {
        var coreOptions = CoreOptionsBuilder.Build();
        _options = coreOptions.AllOptions;
    }

    [Fact]
    public void EmptyLine_ReturnsBranchZero_NoFlags()
    {
        var context = CliContextAnalyzer.Analyze(Array.Empty<string>(), _options);

        Assert.Equal(0, context.CurrentBranchIndex);
        Assert.Empty(context.UsedFlagsInCurrentBranch);
        Assert.Empty(context.KnownAliases);
        Assert.False(context.IsProcessorBranch);
    }

    [Fact]
    public void InputAndAlias_TracksUsedFlags()
    {
        var tokens = new[] { "--input", "pg:orders", "--alias", "b1", "--strategy" };
        var context = CliContextAnalyzer.Analyze(tokens, _options);

        Assert.Contains("--input", context.UsedFlagsInCurrentBranch);
        Assert.Contains("--alias", context.UsedFlagsInCurrentBranch);
        Assert.Contains("--strategy", context.UsedFlagsInCurrentBranch);
        Assert.Equal("pg:", context.CurrentInputPrefix);
        Assert.True(context.IsExpectingFlagValue); // last flag is --strategy and it expects a value
        Assert.Equal("--strategy", context.LastCompletedFlag);
    }

    [Fact]
    public void DagWithProcessor_KnownAliases()
    {
        var tokens = new[] { "--input", "pg:A", "--alias", "a1", "--sql", "duck", "--main" };
        var context = CliContextAnalyzer.Analyze(tokens, _options);

        Assert.Single(context.KnownAliases);
        Assert.Equal("a1", context.KnownAliases[0]);
        Assert.True(context.IsProcessorBranch);
        Assert.Equal("--main", context.LastCompletedFlag);
        Assert.True(context.IsExpectingFlagValue);
    }

    [Fact]
    public void LinearSequence_TwoInputs_BranchIndex1()
    {
        var tokens = new[] { "--input", "pg:A", "-i", "pg:B" };
        var context = CliContextAnalyzer.Analyze(tokens, _options);

        Assert.Equal(1, context.CurrentBranchIndex);
        Assert.False(context.IsProcessorBranch);
    }

    [Fact]
    public void FlagWithoutValue_IsExpectingFlagValue()
    {
        var tokens = new[] { "--strategy" };
        var context = CliContextAnalyzer.Analyze(tokens, _options);

        Assert.True(context.IsExpectingFlagValue);
        Assert.Equal("--strategy", context.LastCompletedFlag);
    }

    [Fact]
    public void MainAndRef_LastFlagIsRef()
    {
        var tokens = new[] { "--input", "pg:A", "--alias", "a1", "--sql", "duck", "--main", "a1", "--ref" };
        var context = CliContextAnalyzer.Analyze(tokens, _options);

        Assert.Equal("--ref", context.LastCompletedFlag);
        Assert.True(context.IsExpectingFlagValue);
    }

    [Fact]
    public void OutputProvided_HasOutputIsTrue()
    {
        var tokens = new[] { "--input", "pg:A", "--output", "out.csv" };
        var context = CliContextAnalyzer.Analyze(tokens, _options);

        Assert.True(context.HasOutput);
    }
}
