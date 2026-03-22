using System.Collections.Generic;
using System.CommandLine;
using System.Linq;
using FluentAssertions;
using DtPipe.Cli;
using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

public class StateMachineAutocompletionTests
{
    private static RootCommand BuildTestRootCommand(out IReadOnlyList<Option> allOptions)
    {
        var opts = CoreOptionsBuilder.Build();
        var all = new List<Option>(opts.AllOptions);

        // Add a mock transformer
        var maskOpt = new Option<string>("--mask");
        all.Add(maskOpt);

        allOptions = all;
        var root = new RootCommand();
        foreach (var o in all) root.Add(o);

        // Add commands
        root.Add(new Command("inspect"));
        root.Add(new Command("providers"));
        root.Add(new Command("secret"));
        root.Add(new Command("completion"));

        return root;
    }

    [Fact]
    public void Phase0_Start_ShowsOnlyStartRules()
    {
        var root = BuildTestRootCommand(out var allOptions);
        var rawWords = new[] { "" };
        int cursorPos = 0;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions, new Dictionary<string, CliPipelinePhase>(), Array.Empty<ICliContributor>()).ToList();

        Assert.Contains("--input", completions);
        Assert.Contains("--job", completions);
        Assert.Contains("inspect", completions);

        // Should NOT contain transformers
        Assert.DoesNotContain("--mask", completions);
        // Should NOT contain output rules
        Assert.DoesNotContain("--strategy", completions);
        // Should NOT contain global rules
        Assert.DoesNotContain("--dry-run", completions);
    }

    [Fact]
    public void Phase1_Input_HidesJobAndHelp_ShowsTransformersAndOutput()
    {
        var root = BuildTestRootCommand(out var allOptions);
        var rawWords = new[] { "--input", "gen:10", "" };
        int cursorPos = 2;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions, new Dictionary<string, CliPipelinePhase>(), Array.Empty<ICliContributor>()).ToList();

        // Should contain Output (Priority) and Transformers
        Assert.Contains("--output", completions);
        Assert.Contains("--mask", completions);
        Assert.Contains("--query", completions); // InputRule

        // Should NOT contain StartRules that are now illegal
        Assert.DoesNotContain("--job", completions);
        Assert.DoesNotContain("inspect", completions);

        // Should NOT contain OutputRules yet
        Assert.DoesNotContain("--strategy", completions);
        Assert.DoesNotContain("--table", completions);
    }

    [Fact]
    public void Phase2_Output_ShowsOutputRulesAndGlobalRules_HidesTransformers()
    {
        var root = BuildTestRootCommand(out var allOptions);
        var rawWords = new[] { "dtpipe", "--input", "gen:10", "--output", "csv:out", "" };
        int cursorPos = 5;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions, new Dictionary<string, CliPipelinePhase>(), Array.Empty<ICliContributor>()).ToList();

        // Should contain OutputRules
        Assert.Contains("--strategy", completions);
        Assert.Contains("--table", completions);

        // Should contain GlobalRules
        Assert.Contains("--dry-run", completions);
        Assert.Contains("--limit", completions);

        // Note: --input is a singleton. If already used in this branch, it's filtered.
        // We verify that Phase 2 logic WOULD allow it if it wasn't blocked by singleton rule.
        // Or we test a scenario where it's not blocked?
        // Actually, the prompt says "possibility to start a new branch: --input".
        // In linear mode, one output usually ends it.

        // Should NOT contain Transformers
        Assert.DoesNotContain("--mask", completions);
        Assert.DoesNotContain("--format", completions);

        // Should NOT contain InputRules
        Assert.DoesNotContain("--query", completions);
    }

    [Fact]
    public void Phase1_SqlProcessor_PrioritizesFromAndRef()
    {
        var root = BuildTestRootCommand(out var allOptions);
        var rawWords = new[] { "--sql", "SELECT 1", "" };
        int cursorPos = 2;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions, new Dictionary<string, CliPipelinePhase>(), Array.Empty<ICliContributor>()).ToList();

        // Processor branch should prioritize --from and --ref (in top 5 high-priority slots)
        Assert.Contains("--from", completions.Take(5));
        Assert.Contains("--ref", completions.Take(5));
    }

    [Fact]
    public void Alias_NoLongerTerminator_StaysInTransformerPhase()
    {
        var root = BuildTestRootCommand(out var allOptions);
        var rawWords = new[] { "dtpipe", "--input", "gen:10", "--alias", "br1", "" };
        int cursorPos = 5;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions, new Dictionary<string, CliPipelinePhase>(), Array.Empty<ICliContributor>()).ToList();

        // After --alias, we should still be in Phase 1 (Transformers allowed, Global options hidden to guide pipeline)
        Assert.DoesNotContain("--dry-run", completions); // Global is hidden in Phase 1
        Assert.Contains("--input", completions);   // New branch start is always allowed

        // Transformers should STILL be available because --alias is not a terminator
        Assert.Contains("--mask", completions);
    }

    [Fact]
    public void NewSourceFlag_StartsNewBranch_ResetsToPhase0()
    {
        var root = BuildTestRootCommand(out var allOptions);
        // Branch 0: input + transform
        // Branch 1: starts with --from (fan-out consumer, cursor after value)
        var rawWords = new[] { "dtpipe", "--input", "gen:10", "--mask", "x", "--from", "br1", "" };
        int cursorPos = 7;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions, new Dictionary<string, CliPipelinePhase>(), Array.Empty<ICliContributor>()).ToList();

        // Branch 1 (after --from br1) is back to Phase 1 (Input/Transform)
        // because hasSeenSourceInCurrentBranch is TRUE but no terminator yet.
        Assert.Contains("--output", completions);
        Assert.Contains("--mask", completions);

        // Should NOT contain StartRules like --job
        Assert.DoesNotContain("--job", completions);
    }
}
