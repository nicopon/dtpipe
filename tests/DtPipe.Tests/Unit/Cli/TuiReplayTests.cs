using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui;
using Spectre.Console;
using Terminal.Gui.Drivers;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The full-screen surface is a presentation, not the record. When it
/// closes, the committed transcript is replayed to real scrollback — the same contract the
/// persistent shell holds (<see cref="AgentShellReplayTests"/>), including on the failure path,
/// so an interrupted run still leaves its partial work behind.
///
/// <para>
/// The ordering matters as much as the content: nothing may be written through Spectre while the
/// toolkit owns the terminal, so the replay happens strictly after teardown.
/// </para>
/// </summary>
[Collection(TerminalGuiCollection.Name)]
public class TuiReplayTests
{
    private static readonly TuiChrome Chrome = new("dtpipe agent · test", "plan · detail: compact");

    private static (TuiApp App, StringWriter Out) Build()
    {
        var sw = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(sw),
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
        });
        console.Profile.Width = 100;
        console.Profile.Height = 30;
        return (new TuiApp(console, DriverRegistry.Names.DOTNET), sw);
    }

    private static (string Clock, string Meter) Header() => ("3s", "42 tok");

    [Fact(Timeout = 30000)]
    public async Task The_Committed_Transcript_Is_Replayed_To_Scrollback_On_Exit()
    {
        var (app, sw) = Build();
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        var done = await app.RunOneTurnAsync(Chrome, log, view, new AgentTrajectory(), Header, async (turnView, _) =>
        {
            turnView.ToolResult("inspect", "{\"cols\":8}", isError: false);
            turnView.AgentResponse("here is the plan");
            await Task.Delay(20);
            return "ok";
        }, CancellationToken.None);

        var output = sw.ToString();
        Assert.Equal("ok", done);
        Assert.Contains("inspect", output);
        Assert.Contains("here is the plan", output);
    }

    [Fact(Timeout = 30000)]
    public async Task The_Transcript_Is_Still_Replayed_When_The_Turn_Throws()
    {
        var (app, sw) = Build();
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            app.RunOneTurnAsync<string>(Chrome, log, view, new AgentTrajectory(), Header, async (turnView, _) =>
            {
                turnView.ToolResult("inspect", "{}", isError: false);
                await Task.Delay(20);
                throw new OperationCanceledException();
            }, CancellationToken.None));

        Assert.Contains("inspect", sw.ToString());
    }

    [Fact(Timeout = 30000)]
    public async Task A_Half_Streamed_Step_Never_Reaches_The_Permanent_Record()
    {
        // The live tail is what the surface shows while tokens arrive. It is not a transcript
        // entry, so an interrupted step leaves the trace line out rather than half of one.
        var (app, sw) = Build();
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            app.RunOneTurnAsync<string>(Chrome, log, view, new AgentTrajectory(), Header, async (turnView, _) =>
            {
                await turnView.StreamingStepAsync(1, 25, AgentDetailLevel.Compact, obs =>
                {
                    obs.OnContent("a partial thought nobody should keep");
                    throw new OperationCanceledException();
                });
                return "unreachable";
            }, CancellationToken.None));

        Assert.DoesNotContain("a partial thought nobody should keep", sw.ToString());
    }

    [Fact(Timeout = 30000)]
    public async Task Nothing_Is_Written_Through_Spectre_Before_The_Surface_Closes()
    {
        // The turn observes the console mid-run: it must still be empty. A Spectre write while the
        // toolkit holds the screen corrupts both.
        var (app, sw) = Build();
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);
        string duringTurn = "(not sampled)";

        await app.RunOneTurnAsync(Chrome, log, view, new AgentTrajectory(), Header, async (turnView, _) =>
        {
            turnView.ToolResult("inspect", "{\"cols\":8}", isError: false);
            await Task.Delay(40);
            duringTurn = sw.ToString();
            return "ok";
        }, CancellationToken.None);

        Assert.Equal(string.Empty, duringTurn);
        Assert.Contains("inspect", sw.ToString());
    }
}
