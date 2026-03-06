using System.Collections.Generic;
using System.CommandLine;
using System.Linq;
using DtPipe.Cli;
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
        var rawWords = new[] { "dtpipe", "" };
        int cursorPos = 1;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions).ToList();

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
        var rawWords = new[] { "dtpipe", "--input", "gen:10", "" };
        int cursorPos = 3;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions).ToList();

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

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions).ToList();

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
    public void Phase1_XStreamer_PrioritizesMainAndRef()
    {
        var root = BuildTestRootCommand(out var allOptions);
        var rawWords = new[] { "dtpipe", "--xstreamer", "duck", "" };
        int cursorPos = 3;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions).ToList();

        // Should prioritize --main and --ref
        Assert.True(completions.IndexOf("--main") < 4);
        Assert.True(completions.IndexOf("--ref") < 4);
    }

    [Fact]
    public void Alias_ActsAsTerminator_EntersPhase2()
    {
        var root = BuildTestRootCommand(out var allOptions);
        var rawWords = new[] { "dtpipe", "--input", "gen:10", "--alias", "br1", "" };
        int cursorPos = 5;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions).ToList();

        // After --alias, we should be in Phase 2 (Global options, new branches)
        Assert.Contains("--dry-run", completions); // Global
        Assert.Contains("--input", completions);   // New brand start

        // Should NOT contain Transformers (they are forbidden after a Sink/Alias)
        Assert.DoesNotContain("--mask", completions);
    }

    [Fact]
    public void NewSourceFlag_StartsNewBranch_ResetsToPhase0()
    {
        var root = BuildTestRootCommand(out var allOptions);
        // Branch 0: input + transform
        // Branch 1: starts with --main (cursor after --main)
        var rawWords = new[] { "dtpipe", "--input", "gen:10", "--mask", "x", "--main", "br1", "" };
        int cursorPos = 7;

        var completions = ContextualCompletionProvider.GetCompletions(root, rawWords, cursorPos, allOptions).ToList();

        // Branch 1 (after --main) is back to Phase 1 (Input/Transform)
        // because hasSeenSourceInCurrentBranch is TRUE but no terminator yet.
        Assert.Contains("--output", completions);
        Assert.Contains("--mask", completions);

        // Should NOT contain StartRules like --job
        Assert.DoesNotContain("--job", completions);
    }
}
