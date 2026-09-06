using System;
using System.Linq;
using DtPipe.Cli.Agent;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The session review is a pure state machine driven by keypresses, so a
/// scripted sequence lands on a deterministic step without a console.
/// </summary>
public class AgentSessionReviewTests
{
    private static AgentTrajectory Trajectory(int steps, params int[] errorAt)
    {
        var t = new AgentTrajectory();
        for (int i = 1; i <= steps; i++)
            t.AddStep(i, $"reasoning {i}", toolName: i % 2 == 0 ? "inspect" : null,
                toolResult: "{}", isError: errorAt.Contains(i));
        return t;
    }

    private static ConsoleKeyInfo Key(ConsoleKey k, char c = '\0')
        => new(c, k, shift: false, alt: false, control: false);

    private static SessionReview Drive(AgentTrajectory t, params ConsoleKeyInfo[] keys)
    {
        var r = new SessionReview(t.Steps);
        foreach (var k in keys)
            if (!r.Apply(k)) break;
        return r;
    }

    [Fact]
    public void Arrows_Move_The_Selection_And_Clamp_At_The_Ends()
    {
        var t = Trajectory(5);

        var r = Drive(t, Key(ConsoleKey.DownArrow), Key(ConsoleKey.DownArrow), Key(ConsoleKey.DownArrow));
        Assert.Equal(3, r.Selected);

        // Clamp at the bottom.
        r = Drive(t, Enumerable.Repeat(Key(ConsoleKey.DownArrow), 20).ToArray());
        Assert.Equal(4, r.Selected);

        // Clamp at the top.
        r = Drive(t, Key(ConsoleKey.DownArrow), Key(ConsoleKey.UpArrow), Key(ConsoleKey.UpArrow), Key(ConsoleKey.UpArrow));
        Assert.Equal(0, r.Selected);
    }

    [Fact]
    public void Home_And_End_Jump_To_The_Edges()
    {
        var t = Trajectory(10);
        Assert.Equal(9, Drive(t, Key(ConsoleKey.End)).Selected);
        Assert.Equal(0, Drive(t, Key(ConsoleKey.End), Key(ConsoleKey.Home)).Selected);
    }

    [Fact]
    public void Right_And_Enter_Expand_A_Step_Left_Collapses_It()
    {
        var t = Trajectory(3);
        Assert.True(Drive(t, Key(ConsoleKey.RightArrow)).Expanded);
        Assert.True(Drive(t, Key(ConsoleKey.Enter)).Expanded);
        Assert.False(Drive(t, Key(ConsoleKey.Enter), Key(ConsoleKey.LeftArrow)).Expanded);
    }

    [Fact]
    public void E_Jumps_To_The_Next_Error_And_B_To_The_Previous()
    {
        var t = Trajectory(10, errorAt: new[] { 4, 8 });

        var r = Drive(t, Key(ConsoleKey.E, 'e'));
        Assert.Equal(3, r.Selected);            // step 4 → index 3
        Assert.True(r.Current!.IsError);

        r = Drive(t, Key(ConsoleKey.E, 'e'), Key(ConsoleKey.E, 'e'));
        Assert.Equal(7, r.Selected);            // step 8 → index 7

        r = Drive(t, Key(ConsoleKey.End), Key(ConsoleKey.B, 'b'));
        Assert.Equal(7, r.Selected);
    }

    [Fact]
    public void Q_And_Escape_Leave_The_Review()
    {
        var t = Trajectory(3);
        Assert.False(new SessionReview(t.Steps).Apply(Key(ConsoleKey.Q)));
        Assert.False(new SessionReview(t.Steps).Apply(Key(ConsoleKey.Escape)));
        Assert.True(new SessionReview(t.Steps).Apply(Key(ConsoleKey.DownArrow)));
    }

    [Fact]
    public void RenderFrame_Shows_The_Header_And_The_Selected_Step()
    {
        var t = Trajectory(4, errorAt: new[] { 2 });
        var review = Drive(t, Key(ConsoleKey.DownArrow));

        var sw = new System.IO.StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(sw),
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
        });
        console.Write(review.RenderFrame());
        var output = sw.ToString();

        Assert.Contains("Session review", output);
        Assert.Contains("2/4", output);
        Assert.Contains("reasoning 2", output);
    }

    [Fact]
    public void An_Empty_Trajectory_Renders_Without_Throwing()
    {
        var review = new SessionReview(new AgentTrajectory().Steps);
        Assert.Null(review.Current);
        Assert.NotNull(review.RenderFrame());
        Assert.True(review.Apply(Key(ConsoleKey.DownArrow)));
    }
}
