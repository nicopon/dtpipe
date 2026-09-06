using System;
using System.Collections.Generic;
using System.IO;
using DtPipe.Cli.Agent;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite 2) lot E5: one summary model, two projections. The scrollback table and the
/// full-screen surface's verdict band both render <see cref="TurnSummaryModel"/>, so the two
/// surfaces cannot disagree about whether a turn succeeded — and the verdict keeps a single author.
///
/// <para>
/// The status has three states, not two: a turn that stopped to ask the user something neither
/// succeeded nor failed (E7). This suite pins the words for all three.
/// </para>
/// </summary>
public class TurnSummaryModelTests
{
    private static readonly Dictionary<string, int> TwoTools =
        new() { ["inspect"] = 2, ["validate-yaml-job"] = 1 };

    private static readonly Dictionary<string, int> NoTools = new();

    private static IAnsiConsole Captured(StringWriter writer)
    {
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(writer),
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
        });
        console.Profile.Width = 120;
        console.Profile.Height = 40;
        return console;
    }

    private static string Render(TurnSummaryModel summary)
    {
        var writer = new StringWriter();
        new AgentTui(Captured(writer)).RenderFinalSummary(summary);
        return writer.ToString();
    }

    private static string RenderReport(TurnSummaryModel summary, AgentMode mode,
        AgentTrajectory? trajectory = null, int maxIterations = 25)
    {
        var writer = new StringWriter();
        new AgentTui(Captured(writer)).ReportTurn(summary, trajectory ?? new AgentTrajectory(), mode, maxIterations);
        return writer.ToString();
    }

    [Fact]
    public void A_Completed_Turn_Renders_Every_Row_Of_The_Scrollback_Table()
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 4, TimeSpan.FromSeconds(12.3456), TwoTools);

        var text = Render(summary);

        Assert.Contains("Session Status", text);
        Assert.Contains("🟢 COMPLETED", text);
        Assert.Contains("delivered a response", text);
        Assert.Contains(summary.DurationText, text);
        Assert.Equal($"{12.3456:F2} seconds", summary.DurationText);   // the run's own culture, as today
        Assert.Contains("inspect: 2, validate-yaml-job: 1", text);
    }

    [Fact]
    public void A_Turn_With_No_Tool_Call_Omits_The_Tool_Row()
    {
        var text = Render(new TurnSummaryModel(TurnOutcome.Succeeded, 1, TimeSpan.FromSeconds(1), new Dictionary<string, int>()));

        Assert.DoesNotContain("Tool Calls", text);
    }

    [Theory]
    [InlineData(TurnOutcome.Succeeded, TurnStatus.Completed, "🟢 COMPLETED", 0)]
    [InlineData(TurnOutcome.AwaitingUserInput, TurnStatus.AwaitingInput, "❓ AWAITING INPUT", 1)]
    [InlineData(TurnOutcome.UserInterrupted, TurnStatus.Incomplete, "❌ INCOMPLETE", 1)]
    [InlineData(TurnOutcome.LlmError, TurnStatus.Incomplete, "❌ INCOMPLETE", 1)]
    [InlineData(TurnOutcome.EmptyResponse, TurnStatus.Incomplete, "❌ INCOMPLETE", 1)]
    [InlineData(TurnOutcome.MaxIterationsReached, TurnStatus.Incomplete, "❌ INCOMPLETE", 1)]
    [InlineData(TurnOutcome.RepetitionDetected, TurnStatus.Incomplete, "❌ INCOMPLETE", 1)]
    public void Every_Outcome_Maps_To_One_Of_Three_States(TurnOutcome outcome, TurnStatus status, string word, int exitCode)
    {
        var summary = new TurnSummaryModel(outcome, 1, TimeSpan.FromSeconds(1), new Dictionary<string, int>());

        Assert.Equal(status, summary.Status);
        Assert.Equal(word, summary.StatusWord);
        Assert.Equal(exitCode, summary.ExitCode);
        Assert.Contains(word, Render(summary));
    }

    [Fact]
    public void Every_Outcome_States_A_Reason_In_Its_Own_Words()
    {
        var reasons = new HashSet<string>();
        foreach (TurnOutcome outcome in Enum.GetValues<TurnOutcome>())
        {
            var reason = TurnSummaryModel.Describe(outcome);
            Assert.NotEqual(outcome.ToString(), reason);   // never the bare enum name
            Assert.True(reasons.Add(reason), $"{outcome} shares its reason with another outcome");
        }
    }

    [Fact]
    public void The_Surface_Verdict_Line_Carries_The_Same_Judgement_As_The_Table()
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 4, TimeSpan.FromSeconds(12.3456), TwoTools);

        var line = summary.VerdictLine();

        Assert.Equal($"🟢 COMPLETED · delivered a response · 4 iterations · {summary.DurationText} · inspect: 2, validate-yaml-job: 1", line);
        Assert.Contains(summary.StatusWord, Render(summary));          // the same word the table prints
        Assert.Contains(summary.Reason, Render(summary));
    }

    [Fact]
    public void A_Pending_Question_Replaces_The_Tally_On_The_Verdict_Line()
    {
        // The band sits directly above the line that answers it: what the user needs there is the
        // question, not how many tools ran.
        var summary = new TurnSummaryModel(TurnOutcome.AwaitingUserInput, 3, TimeSpan.FromSeconds(5), TwoTools,
            Question: "  What should the output file be called?  ");

        Assert.Equal("❓ AWAITING INPUT · What should the output file be called?", summary.VerdictLine());
    }

    [Fact]
    public void One_Iteration_Is_Not_Pluralised()
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 1, TimeSpan.FromSeconds(2), new Dictionary<string, int>());

        Assert.Contains("1 iteration ·", summary.VerdictLine());
    }

    // ── R1: AgentTui.ReportTurn is the one place the four-branch verdict sequence lives, so the
    //    scrollback path and the full-screen session tell the same end of a turn. ──

    [Fact]
    public void ReportTurn_On_A_Completed_Plan_Turn_Prints_The_Table_Then_The_Plan_Next_Steps()
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 3, TimeSpan.FromSeconds(4), NoTools, ProducedPlan: true);

        var text = RenderReport(summary, AgentMode.Plan);

        Assert.Contains("Session Status", text);
        Assert.Contains("Plan ready", text);
        Assert.DoesNotContain("needs your input", text);
        Assert.True(text.IndexOf("Session Status", StringComparison.Ordinal)
            < text.IndexOf("Plan ready", StringComparison.Ordinal),
            "the summary table is printed before the next-steps line");
    }

    [Fact]
    public void ReportTurn_On_A_Completed_Turn_That_Produced_No_Plan_Prints_Only_The_Table()
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 2, TimeSpan.FromSeconds(1), NoTools, ProducedPlan: false);

        var text = RenderReport(summary, AgentMode.Plan);

        Assert.Contains("Session Status", text);
        Assert.DoesNotContain("Plan ready", text);
    }

    [Fact]
    public void ReportTurn_In_Plan_Mode_Ignores_A_Past_Turns_Plan_On_The_Trajectory()
    {
        // The divergence R1 removes: the full-screen session keyed the next-steps line off the
        // trajectory's last YAML — any earlier turn's — while scrollback keyed it off this turn's.
        // A turn that produced nothing must not inherit an earlier turn's "plan ready".
        var trajectory = new AgentTrajectory { LastGeneratedYaml = "jobs:\n  main:\n    input: csv:in.csv\n" };
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 1, TimeSpan.FromSeconds(1), NoTools, ProducedPlan: false);

        var text = RenderReport(summary, AgentMode.Plan, trajectory);

        Assert.DoesNotContain("Plan ready", text);
    }

    [Fact]
    public void ReportTurn_On_A_Pending_Question_Prints_The_Question_And_Neither_Failure_Nor_Plan_Lines()
    {
        var summary = new TurnSummaryModel(TurnOutcome.AwaitingUserInput, 2, TimeSpan.FromSeconds(3), NoTools,
            Question: "What should the output file be called?", ProducedPlan: true);

        var text = RenderReport(summary, AgentMode.Plan);

        Assert.Contains("Session Status", text);
        Assert.Contains("needs your input", text);
        Assert.Contains("What should the output file be called?", text);
        Assert.DoesNotContain("Plan ready", text);
    }

    [Fact]
    public void ReportTurn_On_A_Failure_Prints_The_Guidance_And_Neither_Question_Nor_Plan_Lines()
    {
        var summary = new TurnSummaryModel(TurnOutcome.LlmError, 1, TimeSpan.FromSeconds(1), NoTools, ProducedPlan: true);

        var text = RenderReport(summary, AgentMode.Plan);

        Assert.Contains("Session Status", text);
        Assert.Contains("The LLM call failed", text);
        Assert.DoesNotContain("needs your input", text);
        Assert.DoesNotContain("Plan ready", text);
    }

    [Fact]
    public void ReportTurn_Withholds_The_Plan_Next_Steps_Outside_Plan_Mode()
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 1, TimeSpan.FromSeconds(1), NoTools, ProducedPlan: true);

        var text = RenderReport(summary, AgentMode.Execute);

        Assert.DoesNotContain("Plan ready", text);
    }
}
