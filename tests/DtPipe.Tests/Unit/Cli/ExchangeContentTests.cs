using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The exchange the full-screen surface shows: which marker, and a headline that names what the
/// user is expected to contribute next. Pure — no terminal, no model.
/// </summary>
public class ExchangeContentTests
{
    private static readonly IReadOnlyDictionary<string, int> TwoTools =
        new Dictionary<string, int> { ["inspect"] = 2, ["validate-yaml-job"] = 1 };

    [Fact]
    public void A_Question_Asks_For_An_Answer_And_Carries_It_In_The_Body()
    {
        var summary = new TurnSummaryModel(TurnOutcome.AwaitingUserInput, 3, TimeSpan.FromSeconds(5), TwoTools,
            Question: new AgentQuestion("  What should the output file be called?  "));

        var exchange = ExchangeContent.Of(summary, hasPlan: false);

        Assert.Equal(ExchangeKind.Question, exchange.Kind);
        Assert.Equal("?", exchange.Marker);
        Assert.Contains("answer below", exchange.Headline);
        Assert.Equal("What should the output file be called?", exchange.Body);
    }

    /// <summary>
    /// A turn that ended on a sentence shows the sentence. It used to leave a tally in its place,
    /// so a question asked in prose — which a weak model does instead of calling the tool — sat
    /// under a headline reading DONE, reachable only by selecting the last step.
    /// </summary>
    [Fact]
    public void A_Finished_Turn_Shows_What_The_Agent_Said()
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 2, TimeSpan.FromSeconds(4), TwoTools,
            ClosingWords: "  Quel nom de fichier SQLite souhaitez-vous utiliser ?  ");

        var exchange = ExchangeContent.Of(summary, hasPlan: false);

        Assert.StartsWith("Quel nom de fichier SQLite souhaitez-vous utiliser ?", exchange.Body);
        Assert.Contains("2 steps", exchange.Body);
    }

    /// <summary>A turn that ended on a tool call has no closing words; the tally is what there is.</summary>
    [Fact]
    public void A_Finished_Turn_Without_Words_Still_Shows_Its_Tally()
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 2, TimeSpan.FromSeconds(4), TwoTools);

        Assert.StartsWith("2 steps", ExchangeContent.Of(summary, hasPlan: false).Body);
    }

    /// <summary>
    /// The choices the model offered belong to the question. The band's body is a scrollable list,
    /// so each choice is its own line and the headline says an answer may be one of them.
    /// </summary>
    [Fact]
    public void The_Choices_The_Model_Offered_Are_Shown_Under_The_Question()
    {
        var summary = new TurnSummaryModel(TurnOutcome.AwaitingUserInput, 1, TimeSpan.FromSeconds(1), TwoTools,
            Question: new AgentQuestion("Which column keys the upsert?", new[] { "order_id", "customer_id" }));

        var exchange = ExchangeContent.Of(summary, hasPlan: false);

        Assert.Contains("pick one", exchange.Headline);
        Assert.Equal(
            new[] { "Which column keys the upsert?", "  1. order_id", "  2. customer_id" },
            exchange.BodyLines());
    }

    /// <summary>
    /// A turn that delivered says what can be done with what it delivered — running a plan is not
    /// offered when there is no plan to run.
    /// </summary>
    [Theory]
    [InlineData(true, "/exec")]
    [InlineData(false, "anything else")]
    public void A_Delivered_Turn_Names_The_Next_Move(bool hasPlan, string expected)
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 4, TimeSpan.FromSeconds(12.3), TwoTools,
            Tokens: 1234);

        var exchange = ExchangeContent.Of(summary, hasPlan);

        Assert.Equal(ExchangeKind.Done, exchange.Kind);
        Assert.Equal("✓", exchange.Marker);
        Assert.Contains(expected, exchange.Headline);
        Assert.Contains("4 steps", exchange.Body);
        Assert.Contains("3 tool calls", exchange.Body);      // the total, not the breakdown
        Assert.Contains("1234 tok", exchange.Body);
        Assert.DoesNotContain("inspect: 2", exchange.Body);  // which tools ran is the steps list's job
    }

    /// <summary>A turn that called nothing and reported nothing says neither.</summary>
    [Fact]
    public void An_Absent_Count_Is_Left_Out_Rather_Than_Printed_As_Zero()
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 2, TimeSpan.FromSeconds(3),
            new Dictionary<string, int>());

        var body = ExchangeContent.Of(summary, hasPlan: false).Body;

        Assert.DoesNotContain("tool call", body);
        Assert.DoesNotContain("tok", body);
    }

    [Theory]
    [InlineData(TurnOutcome.MaxIterationsReached)]
    [InlineData(TurnOutcome.LlmError)]
    [InlineData(TurnOutcome.EmptyResponse)]
    [InlineData(TurnOutcome.RepetitionDetected)]
    [InlineData(TurnOutcome.UserInterrupted)]
    public void Every_Unfinished_Turn_Is_Stopped_And_Says_Why(TurnOutcome outcome)
    {
        var summary = new TurnSummaryModel(outcome, 6, TimeSpan.FromSeconds(48), TwoTools);

        var exchange = ExchangeContent.Of(summary, hasPlan: false);

        Assert.Equal(ExchangeKind.Stopped, exchange.Kind);
        Assert.Equal("✗", exchange.Marker);
        Assert.Contains(TurnSummaryModel.Describe(outcome), exchange.Headline);
    }

    [Fact]
    public void One_Step_Is_Not_Pluralised()
    {
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 1, TimeSpan.FromSeconds(2), new Dictionary<string, int>());

        Assert.Contains("1 step ·", ExchangeContent.Of(summary, hasPlan: false).Body);
    }

    [Fact]
    public void The_Session_Speaking_For_Itself_Is_A_Note_With_No_Body()
    {
        var exchange = ExchangeContent.Note("  Saved the plan to ./plan.yaml  ");

        Assert.Equal(ExchangeKind.Note, exchange.Kind);
        Assert.Equal("•", exchange.Marker);
        Assert.Equal("Saved the plan to ./plan.yaml", exchange.Headline);
        Assert.Empty(exchange.BodyLines());
    }

    /// <summary>
    /// A running turn says only that it is running. What the model is producing belongs to the step
    /// producing it — the detail panel shows that — and putting it here too would put two live
    /// streams on one screen, which is what retiring the transcript band was for.
    /// </summary>
    [Fact]
    public void A_Running_Turn_Says_So_And_Carries_No_Stream()
    {
        var exchange = ExchangeContent.Working();

        Assert.Equal(ExchangeKind.Working, exchange.Kind);
        Assert.Equal("▸", exchange.Marker);
        Assert.Contains("esc to stop", exchange.Headline);
        Assert.Empty(exchange.BodyLines());
    }

    /// <summary>
    /// Every marker shares its line with text in a fixed grid, so none of them may be an emoji: a
    /// terminal draws an astral character two cells wide and it swallows the space after itself.
    /// <see cref="TurnSummaryModel.StatusWord"/> spells the same states with emoji on purpose — that
    /// one goes into a stream, where width does not matter.
    /// </summary>
    [Fact]
    public void No_Marker_Is_An_Emoji()
    {
        var markers = Enum.GetValues<ExchangeKind>()
            .Select(kind => new Exchange(kind, "headline", "body").Marker);

        foreach (var marker in markers)
        {
            Assert.DoesNotContain(marker, char.IsSurrogate);
            Assert.Single(marker);
        }
    }
}
