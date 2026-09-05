using System;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite) lot D3: the persistent shell frame is auto-cleared on exit, so the permanent
/// record is the transcript replayed to scrollback — not a frozen frame with a footer and an input
/// line stuck in it. The final summary then prints below, still the sole verdict.
/// </summary>
public class AgentShellReplayTests
{
    [Fact]
    public async Task The_Committed_Transcript_Is_Replayed_To_Scrollback_On_Exit()
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
        console.Profile.Height = 30;
        var tui = new AgentTui(console);

        var shell = new AgentShell { Status = "plan · detail: compact" };
        var done = await tui.RunInLiveShellAsync(
            shell,
            () => ("3s", "42 tok"),
            view =>
            {
                view.ToolResult("inspect", "{\"cols\":8}", false);
                view.AgentResponse("here is the plan");
                return Task.FromResult("ok");
            });

        var output = sw.ToString();
        Assert.Equal("ok", done);
        // The committed lines are in scrollback…
        Assert.Contains("inspect", output);
        Assert.Contains("here is the plan", output);
        // …and the input-line glyph is not (the footer never lands in the permanent record).
        Assert.DoesNotContain("\n› \n", output);
    }

    [Fact]
    public async Task The_Transcript_Is_Still_Replayed_When_The_Body_Throws()
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
        console.Profile.Height = 30;
        var tui = new AgentTui(console);
        var shell = new AgentShell();

        await Assert.ThrowsAsync<OperationCanceledException>(() => tui.RunInLiveShellAsync<string>(
            shell,
            () => ("1s", ""),
            view =>
            {
                view.ToolResult("inspect", "{}", false);
                throw new OperationCanceledException();
            }));

        Assert.Contains("inspect", sw.ToString());
    }
}
