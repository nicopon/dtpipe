using DtPipe.Cli.Pipeline;
using Xunit;
using System.Collections.Generic;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// F6 — exhaustive table over all branch-split states × trigger tokens.
/// </summary>
public class BranchSplitDecisionTests
{
    public static IEnumerable<object[]> AllStates()
    {
        for (int i = 0; i < 8; i++)
            yield return new object[] { (i & 1) != 0, (i & 2) != 0, (i & 4) != 0 };
    }

    [Theory]
    [MemberData(nameof(AllStates))]
    public void Second_Input_Splits_Only_When_Input_Or_Job_Already_Seen(bool hasInput, bool hasJob, bool hasFrom)
    {
        var s = new BranchSplitState(hasInput, hasJob, hasFrom);
        var expected = (hasInput || hasJob) ? SplitDecision.NewInput : SplitDecision.Stay;

        Assert.Equal(expected, BranchSplitDecision.Decide(s, "--input"));
        Assert.Equal(expected, BranchSplitDecision.Decide(s, "-i"));
    }

    [Theory]
    [MemberData(nameof(AllStates))]
    public void From_Splits_When_Any_Branch_Anchor_Already_Seen(bool hasInput, bool hasJob, bool hasFrom)
    {
        var s = new BranchSplitState(hasInput, hasJob, hasFrom);
        var expected = (hasFrom || hasInput || hasJob) ? SplitDecision.NewFrom : SplitDecision.Stay;

        // First --from in a fresh branch stays in the current branch.
        Assert.Equal(expected, BranchSplitDecision.Decide(s, "--from"));
    }

    [Theory]
    [MemberData(nameof(AllStates))]
    public void Second_Job_Splits_Only_When_Job_Or_Input_Already_Seen(bool hasInput, bool hasJob, bool hasFrom)
    {
        var s = new BranchSplitState(hasInput, hasJob, hasFrom);
        var expected = (hasJob || hasInput) ? SplitDecision.NewJob : SplitDecision.Stay;

        Assert.Equal(expected, BranchSplitDecision.Decide(s, "--job"));
        Assert.Equal(expected, BranchSplitDecision.Decide(s, "-j"));
    }

    [Theory]
    [MemberData(nameof(AllStates))]
    public void Ref_Splits_After_An_Input_Or_Job_Unless_The_Branch_Already_Has_A_From(bool hasInput, bool hasJob, bool hasFrom)
    {
        var s = new BranchSplitState(hasInput, hasJob, hasFrom);
        var expected = (hasInput || hasJob) && !hasFrom ? SplitDecision.NewRef : SplitDecision.Stay;

        Assert.Equal(expected, BranchSplitDecision.Decide(s, "--ref"));
    }

    [Fact]
    public void A_Branch_Opened_By_Ref_Keeps_A_Second_Ref_So_It_Is_Refused_Not_Split()
    {
        // A branch opened by --ref carries no input, no job and no from.
        var openedByRef = new BranchSplitState(HasInput: false, HasJob: false, HasFrom: false);

        Assert.Equal(SplitDecision.Stay, BranchSplitDecision.Decide(openedByRef, "--ref"));
    }

    [Fact]
    public void Non_Trigger_Tokens_Never_Split()
    {
        var s = new BranchSplitState(HasInput: true, HasJob: true, HasFrom: true);

        foreach (var token in new[] { "--sql", "--merge", "-o", "--output", "--alias", "positional", "--filter", "--strict-bindings" })
            Assert.Equal(SplitDecision.Stay, BranchSplitDecision.Decide(s, token));
    }
}
