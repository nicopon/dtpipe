using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite 2) lot E3: "jump to the next / previous error" is one function, so it behaves
/// the same in the scrollback review and the full-screen steps panel.
/// </summary>
public class StepNavigationTests
{
    private static IReadOnlyList<TrajectoryStep> Steps(params int[] errorAt)
        => Enumerable.Range(0, 10)
            .Select(i => new TrajectoryStep { Iteration = i + 1, IsError = errorAt.Contains(i) })
            .ToArray();

    [Fact]
    public void Finds_The_Next_Error_Forward()
    {
        var s = Steps(3, 7);
        Assert.Equal(3, StepNavigation.NextError(s, 0, +1));
        Assert.Equal(7, StepNavigation.NextError(s, 3, +1));
    }

    [Fact]
    public void Finds_The_Previous_Error_Backward()
    {
        var s = Steps(3, 7);
        Assert.Equal(7, StepNavigation.NextError(s, 9, -1));
        Assert.Equal(3, StepNavigation.NextError(s, 7, -1));
    }

    [Fact]
    public void Stays_Put_When_There_Is_No_Further_Error()
    {
        var s = Steps(3);
        Assert.Equal(5, StepNavigation.NextError(s, 5, +1));   // nothing after 3
        Assert.Equal(1, StepNavigation.NextError(s, 1, -1));   // nothing before 3
    }

    [Fact]
    public void Never_Lands_On_The_Current_Step()
    {
        var s = Steps(4);
        Assert.Equal(4, StepNavigation.NextError(s, 4, +1));   // the current step is an error, but we move off it
        Assert.Equal(4, StepNavigation.NextError(s, 4, -1));
    }

    [Fact]
    public void Handles_An_Empty_List()
    {
        Assert.Equal(0, StepNavigation.NextError(System.Array.Empty<TrajectoryStep>(), 0, +1));
    }
}
