using System.CommandLine;
using DtPipe.Cli;
using DtPipe.Cli.Infrastructure;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

public class ContextualCompletionProviderTests
{
    private static RootCommand BuildTestRootCommand(out IReadOnlyList<Option> allOptions)
    {
        var opts = CoreOptionsBuilder.Build();
        allOptions = opts.AllOptions;
        var root = new RootCommand();
        foreach (var o in opts.AllOptions) root.Add(o);
        return root;
    }

    [Fact]
    public void AfterMainFlag_ReturnsOnlyKnownAliases()
    {
        var root = BuildTestRootCommand(out var allOptions);
        var rawWords = new[] { "dtpipe", "--input", "pg:A", "--alias", "brA", "-x", "duck", "--main", "" };
        int cursorPos = 8; // index of the empty string

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions, new Dictionary<string, CliPipelinePhase>(), Array.Empty<ICliContributor>());

        Assert.Single(completions);
        Assert.Equal("brA", completions.First());
    }

    [Fact]
    public void UsedFlag_IsFilteredOut()
    {
        var root = BuildTestRootCommand(out var allOptions);
        // User already typed --strategy Append, and is now typing --stra
        var rawWords = new[] { "dtpipe", "--strategy", "Append", "--stra" };
        int cursorPos = 3;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions, new Dictionary<string, CliPipelinePhase>(), Array.Empty<ICliContributor>()).ToList();

        Assert.DoesNotContain("--strategy", completions);
        Assert.DoesNotContain("-s", completions);
    }

    [Fact]
    public void NoAlias_AfterMain_ReturnsEmpty()
    {
        var root = BuildTestRootCommand(out var allOptions);
        var rawWords = new[] { "dtpipe", "-x", "duck", "--main", "" };
        int cursorPos = 4;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions, new Dictionary<string, CliPipelinePhase>(), Array.Empty<ICliContributor>());

        Assert.Empty(completions);
    }

    [Fact]
    public void LinearGuide_PrioritizesOutput()
    {
        var root = BuildTestRootCommand(out var allOptions);
        // User provided --input, now typing next token
        var rawWords = new[] { "dtpipe", "--input", "pg:orders", "" };
        int cursorPos = 3;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions, new Dictionary<string, CliPipelinePhase>(), Array.Empty<ICliContributor>()).ToList();

        // --output should be in the first 2 slots
        Assert.True(completions.IndexOf("--output") < 2);
    }

    [Fact]
    public void FirstTab_OnlySuggestsStartOptions()
    {
        var root = BuildTestRootCommand(out var allOptions);
        // User hasn't typed any flag yet
        var rawWords = new[] { "dtpipe", "" };
        int cursorPos = 1;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions, new Dictionary<string, CliPipelinePhase>(), Array.Empty<ICliContributor>()).ToList();

        // Should contain input
        Assert.Contains("--input", completions);
        Assert.DoesNotContain("-i", completions);
        Assert.Contains("--job", completions);

        // Should NOT contain transformers, formats, etc
        Assert.DoesNotContain("--format", completions);
        Assert.DoesNotContain("--strategy", completions);
        Assert.DoesNotContain("--filter", completions);
        Assert.DoesNotContain("-x", completions);
        Assert.DoesNotContain("engine-duckdb", completions);
    }

    [Fact]
    public void AfterOutput_HidesTransformers()
    {
        var root = BuildTestRootCommand(out var allOptions);
        // User provided --input and --output, now typing next token
        var rawWords = new[] { "dtpipe", "--input", "pg:orders", "--output", "csv:out.csv", "" };
        int cursorPos = 5;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions, new Dictionary<string, CliPipelinePhase>(), Array.Empty<ICliContributor>()).ToList();

        // Should NOT contain transformers
        Assert.DoesNotContain("--format", completions);
        Assert.DoesNotContain("--filter", completions);
        Assert.DoesNotContain("--mask", completions);

        // Should still contain writer options
        Assert.Contains("--strategy", completions);
        Assert.Contains("--table", completions);
    }
}
