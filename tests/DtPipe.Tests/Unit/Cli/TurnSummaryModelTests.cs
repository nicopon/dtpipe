using System;
using System.Collections.Generic;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// One summary model, read by every host that shows a turn, so no two of them can disagree about
/// whether a turn succeeded — and the verdict keeps a single author.
///
/// <para>
/// The status has three states, not two: a turn that stopped to ask the user something neither
/// succeeded nor failed (E7). This suite pins the words for all three.
/// </para>
/// </summary>
[Collection("console-serial")]
public class TurnSummaryModelTests
{
    private static readonly Dictionary<string, int> TwoTools =
        new() { ["inspect"] = 2, ["validate-yaml-job"] = 1 };

    [Fact]
    public void A_Completed_Turn_States_Its_Duration_And_Its_Tool_Tally()
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 4, TimeSpan.FromSeconds(12.3456), TwoTools);

        Assert.Equal($"{12.3456:F2} seconds", summary.DurationText);   // the run's own culture
        Assert.Equal("inspect: 2, validate-yaml-job: 1", summary.ToolSummary);
        Assert.Equal(3, summary.TotalToolCalls);
        Assert.Equal("delivered a response", summary.Reason);
    }

    [Fact]
    public void A_Turn_With_No_Tool_Call_Has_No_Tool_Tally()
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 1, TimeSpan.FromSeconds(1), new Dictionary<string, int>());

        Assert.Null(summary.ToolSummary);
        Assert.Equal(0, summary.TotalToolCalls);
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
}
