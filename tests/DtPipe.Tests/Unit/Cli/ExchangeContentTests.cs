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
            Question: "  What should the output file be called?  ");

        var exchange = ExchangeContent.Of(summary, hasPlan: false);

        Assert.Equal(ExchangeKind.Question, exchange.Kind);
        Assert.Equal("?", exchange.Marker);
        Assert.Contains("answer below", exchange.Headline);
        Assert.Equal("What should the output file be called?", exchange.Body);
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
        var summary = new TurnSummaryModel(TurnOutcome.Succeeded, 4, TimeSpan.FromSeconds(12.3), TwoTools);

        var exchange = ExchangeContent.Of(summary, hasPlan);

        Assert.Equal(ExchangeKind.Done, exchange.Kind);
        Assert.Equal("✓", exchange.Marker);
        Assert.Contains(expected, exchange.Headline);
        Assert.Contains("4 steps", exchange.Body);
        Assert.Contains("inspect: 2", exchange.Body);
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

    [Fact]
    public void A_Running_Turn_Puts_What_The_Model_Has_Said_In_The_Body()
    {
        var exchange = ExchangeContent.Speaking("first line\nsecond line");

        Assert.Equal(ExchangeKind.Speaking, exchange.Kind);
        Assert.Equal("▸", exchange.Marker);
        Assert.Equal(new[] { "first line", "second line" }, exchange.BodyLines());
        Assert.Empty(ExchangeContent.Speaking(null).BodyLines());
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
