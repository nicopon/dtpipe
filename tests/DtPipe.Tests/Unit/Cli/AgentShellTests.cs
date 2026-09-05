using System.Linq;
using DtPipe.Cli.Agent;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite) lot D2: the persistent shell frame is a pure function of its state and the
/// height budget — the header, the transcript tail, the status/hint/input rows compose the same
/// way every time and the transcript never overflows the terminal.
/// </summary>
public class AgentShellTests
{
    private static string Render(AgentShell shell, int height)
    {
        var sw = new System.IO.StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(sw),
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
        });
        console.Profile.Width = 100;
        console.Write(shell.RenderFrame(height));
        return sw.ToString();
    }

    [Fact]
    public void The_Frame_Carries_The_Header_Status_Hints_And_Input()
    {
        var shell = new AgentShell
        {
            Title = "dtpipe agent",
            Subtitle = "gemma4:12b · plan",
            Clock = "2m14s",
            Meter = "3.2k tok",
            Status = "plan · dry-run · detail:peek",
            Hints = "esc stop · tab mode",
            InputLine = "[grey]>[/] inspect",
        };
        shell.Append("first step");

        var output = Render(shell, height: 20);

        Assert.Contains("dtpipe agent", output);
        Assert.Contains("gemma4:12b", output);
        Assert.Contains("2m14s", output);
        Assert.Contains("plan · dry-run · detail:peek", output);
        Assert.Contains("esc stop · tab mode", output);
        Assert.Contains("inspect", output);
        Assert.Contains("first step", output);
    }

    [Fact]
    public void The_Transcript_Never_Overflows_The_Height_Budget()
    {
        var shell = new AgentShell();
        for (int i = 0; i < 60; i++) shell.Append($"line {i}");

        var body = shell.VisibleBodyLines(10);

        Assert.Equal(10, body.Count);
        Assert.Equal("line 59", body[^1]);
        Assert.Equal("line 50", body[0]);
    }

    [Fact]
    public void The_Live_Tail_Is_Shown_Below_The_Committed_Transcript()
    {
        var shell = new AgentShell();
        shell.Append("committed 1");
        shell.Append("committed 2");
        shell.LiveTail = "streaming line a\nstreaming line b";

        var body = shell.VisibleBodyLines(10);

        Assert.Equal(new[] { "committed 1", "committed 2", "streaming line a", "streaming line b" }, body);
    }

    [Fact]
    public void A_Short_Transcript_Renders_Without_Padding_Or_Throwing()
    {
        var shell = new AgentShell { Status = "plan" };

        var output = Render(shell, height: 24);

        Assert.Contains("plan", output);
    }

    [Fact]
    public void A_Tiny_Height_Renders_The_Most_Recent_Lines_Without_Throwing()
    {
        var shell = new AgentShell { Status = "plan" };
        for (int i = 0; i < 20; i++) shell.Append($"l{i}");

        var output = Render(shell, height: 4);

        Assert.Contains("l19", output);
        Assert.DoesNotContain("l0\n", output);
    }
}
