using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui;
using Spectre.Console;
using Terminal.Gui.Drivers;
using Terminal.Gui.Input;
using Terminal.Gui.Testing;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite 2) lot E2: the full-screen surface really starts, runs a turn on a worker
/// thread, and hands the terminal back — driven headless by the pure-ANSI driver, which needs no
/// TTY. These are the only tests here that start a toolkit application; everything else about the
/// surface is asserted on the pure model.
/// </summary>
[Collection(TerminalGuiCollection.Name)]
public class TuiSmokeTests
{
    private static readonly TuiChrome Chrome = new("dtpipe agent · test", "plan · detail: compact", "^C quit");

    private static (TuiApp App, IAnsiConsole Console, StringWriter Out) Build()
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
        return (new TuiApp(console, DriverRegistry.Names.ANSI), console, sw);
    }

    private static (string Clock, string Meter) Header() => ("3s", "42 tok");

    /// <summary>The driver's cell buffer as text — what the surface actually put on screen.</summary>
    private static string Flatten(IDriver driver)
    {
        var cells = driver.Contents;
        if (cells is null) return string.Empty;

        var sb = new StringBuilder();
        for (int row = 0; row < cells.GetLength(0); row++)
        {
            for (int col = 0; col < cells.GetLength(1); col++)
                sb.Append(cells[row, col].Grapheme);
            sb.Append('\n');
        }
        return sb.ToString();
    }

    [Fact(Timeout = 30000)]
    public async Task The_Surface_Draws_The_Title_The_Transcript_And_The_Status()
    {
        // Reading the cell buffer is the only way to assert the surface without a human at a
        // terminal — and a captured pty cannot stand in for one, because no emulator is there to
        // answer the toolkit's capability queries, so the transcript never reaches the capture.
        var (app, _, _) = Build();
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);
        string? screen = null;

        await app.RunTurnAsync(Chrome, log, view, Header,
            async (turnView, _) =>
            {
                turnView.ToolResult("inspect", "UNIQUEMARKER42", isError: false);
                await Task.Delay(500);
                return "ok";
            },
            CancellationToken.None,
            surfaceReady: live => live.AddTimeout(TimeSpan.FromMilliseconds(300), () =>
            {
                // Sampled on the UI thread, where the cell buffer is safe to read.
                screen ??= Flatten(live.Driver!);
                return false;
            }));

        Assert.NotNull(screen);
        Assert.Contains("dtpipe agent", screen);            // the window title
        Assert.Contains("UNIQUEMARKER42", screen);          // a committed transcript entry
        Assert.Contains("plan · detail: compact", screen);  // the status line
        Assert.Contains("3s", screen);                      // the polled clock
    }

    [Fact(Timeout = 30000)]
    public async Task The_Surface_Opens_Runs_A_Turn_And_Closes()
    {
        var (app, _, _) = Build();
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        var result = await app.RunTurnAsync(Chrome, log, view, Header, async (turnView, _) =>
        {
            turnView.ToolResult("inspect", "{\"cols\":8}", isError: false);
            await Task.Delay(30);
            turnView.AgentResponse("here is the plan");
            return "done";
        }, CancellationToken.None);

        Assert.Equal("done", result);
        Assert.Equal(2, log.Entries.Count);
    }

    [Fact(Timeout = 30000)]
    public async Task A_Turn_That_Ends_Instantly_Does_Not_Deadlock_The_Loop()
    {
        // The stop can be requested before the loop has started — an LLM error returns on the
        // first call. The request must survive until the first iteration, or Run never returns.
        var (app, _, _) = Build();
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        var result = await app.RunTurnAsync(Chrome, log, view, Header,
            (_, _) => Task.FromResult(7), CancellationToken.None);

        Assert.Equal(7, result);
    }

    [Fact(Timeout = 30000)]
    public async Task Two_Turns_Can_Run_One_After_The_Other_In_One_Process()
    {
        // The post-mission loop runs several turns; each opens and closes its own application.
        var (app, _, _) = Build();

        for (int i = 1; i <= 2; i++)
        {
            var log = new TranscriptLog();
            var view = new TuiTurnView(log);
            var n = await app.RunTurnAsync(Chrome, log, view, Header, async (turnView, _) =>
            {
                turnView.ToolResult("inspect", $"turn {i}", isError: false);
                await Task.Delay(20);
                return i;
            }, CancellationToken.None);

            Assert.Equal(i, n);
            Assert.Single(log.Entries);
        }
    }

    [Fact(Timeout = 30000)]
    public async Task A_Failing_Turn_Surfaces_Its_Exception_After_The_Surface_Closes()
    {
        var (app, _, _) = Build();
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            app.RunTurnAsync<string>(Chrome, log, view, Header, async (turnView, _) =>
            {
                turnView.ToolResult("inspect", "{}", isError: false);
                await Task.Delay(20);
                throw new InvalidOperationException("boom");
            }, CancellationToken.None));
    }

    [Fact(Timeout = 30000)]
    public async Task Ctrl_C_Cancels_The_Turn_So_The_Run_Can_Report_130()
    {
        // Raw mode swallows SIGINT, so Ctrl+C reaches the surface as a keystroke. Nothing else
        // turns it back into an interrupt: if this path breaks, an interrupted run reports success
        // instead of the POSIX 130 the agent command derives from this cancellation (F16).
        var (app, _, _) = Build();
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            app.RunTurnAsync<int>(Chrome, log, view, Header,
                async (_, turnCt) =>
                {
                    await Task.Delay(10_000, turnCt);
                    return 1;
                },
                CancellationToken.None,
                surfaceReady: live => live.InjectKey(Key.C.WithCtrl)));
    }

    [Fact(Timeout = 30000)]
    public async Task Engine_Console_Writes_Are_Held_Back_Until_The_Terminal_Is_Handed_Back()
    {
        // The engine writes diagnostics straight to stderr (an unbound-options warning inside a
        // tool call, say). During the session that would print over a screen the toolkit owns.
        var (app, _, _) = Build();
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        var captured = new StringWriter();
        var realError = Console.Error;
        Console.SetError(captured);
        try
        {
            await app.RunTurnAsync(Chrome, log, view, Header, async (_, _) =>
            {
                await Task.Delay(20);
                Console.Error.WriteLine("[dtpipe] Warning: something the engine wanted to say");
                // Still inside the session: the real stderr must not have seen it yet.
                Assert.DoesNotContain("something the engine wanted to say", captured.ToString());
                return "ok";
            }, CancellationToken.None);
        }
        finally
        {
            Console.SetError(realError);
        }

        // Released once the terminal is back.
        Assert.Contains("something the engine wanted to say", captured.ToString());
    }

    [Fact(Timeout = 30000)]
    public async Task A_Spectre_Console_Write_Is_Held_Back_The_Same_Way_A_Direct_One_Is()
    {
        // E2b: the engine renders through the DI IAnsiConsole, not Console.Error directly. That
        // console is built to forward to the current stderr, so the session's quarantine catches
        // it too — otherwise a topology panel or an error markup lands mid-screen.
        var (app, _, _) = Build();
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        var captured = new StringWriter();
        var realError = Console.Error;
        Console.SetError(captured);
        try
        {
            var engineConsole = DtPipe.Cli.Infrastructure.SharedConsole.Create();
            await app.RunTurnAsync(Chrome, log, view, Header, async (_, _) =>
            {
                await Task.Delay(20);
                engineConsole.MarkupLine("[yellow]Pipeline Execution Plan[/]");
                Assert.DoesNotContain("Pipeline Execution Plan", captured.ToString());
                return "ok";
            }, CancellationToken.None);
        }
        finally
        {
            Console.SetError(realError);
        }

        Assert.Contains("Pipeline Execution Plan", captured.ToString());
    }

    [Fact(Timeout = 30000)]
    public async Task A_Cancelled_Caller_Token_Cancels_The_Turn()
    {
        // The same shape Ctrl+C produces: the turn's token trips, the body throws, and the caller
        // sees an OperationCanceledException — which the agent command reports as exit 130 (F16).
        var (app, _, _) = Build();
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);
        using var cts = new CancellationTokenSource();

        var task = app.RunTurnAsync<int>(Chrome, log, view, Header, async (_, turnCt) =>
        {
            cts.Cancel();
            await Task.Delay(10_000, turnCt);
            return 1;
        }, cts.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
    }
}
