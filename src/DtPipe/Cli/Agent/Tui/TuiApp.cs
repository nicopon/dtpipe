using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Terminal.Gui.App;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

namespace DtPipe.Cli.Agent.Tui;

/// <summary>
/// Runs one agent turn inside a Terminal.Gui application that owns the screen, then hands the
/// terminal back and replays the transcript to real scrollback. The full-screen surface is a
/// presentation of the turn; the permanent record stays the scrollback lines, and the verdict
/// stays <see cref="AgentTui.RenderFinalSummary"/>'s alone.
///
/// <para>
/// Two rules hold this together and both are easy to break:
/// </para>
/// <list type="bullet">
/// <item>
/// <b>Nothing writes through Spectre while the toolkit owns the terminal.</b> The replay happens
/// after <c>Dispose</c> has restored it — the ordering in the <c>finally</c> below is the contract,
/// not a preference.
/// </item>
/// <item>
/// <b>The repaint is a poll.</b> The turn thread only mutates the <see cref="TranscriptLog"/>; a
/// timer on the UI thread notices. Marshalling one call per streamed token would repaint at the
/// model's token rate.
/// </item>
/// </list>
/// </summary>
internal sealed class TuiApp
{
    private static readonly TimeSpan RepaintInterval = TimeSpan.FromMilliseconds(100);

    private readonly IAnsiConsole _console;
    private readonly string? _driverName;

    /// <param name="console">Where the transcript is replayed once the terminal is handed back.</param>
    /// <param name="driverName">
    /// Toolkit driver to force, or null to let it pick per platform (ANSI on Unix — the right
    /// driver for a real terminal). Tests pass the managed <c>dotnet</c> driver: it goes through
    /// <c>System.Console</c>, so a redirected console (as the test host provides) fully contains it
    /// and it never writes a capability query or an alternate-screen switch to a real terminal —
    /// the ANSI driver talks to fd 1 directly and would, polluting a developer's shell.
    /// </param>
    public TuiApp(IAnsiConsole console, string? driverName = null)
    {
        _console = console;
        _driverName = driverName;
    }

    /// <summary>
    /// Opens the surface, runs <paramref name="body"/> on a worker thread, and closes it. The
    /// worker's exception — including the cancellation Ctrl+C raises — propagates to the caller
    /// after the terminal has been restored and the transcript replayed, so an interrupted run
    /// still reports 130 and still leaves its partial record behind.
    /// </summary>
    /// <param name="chrome">The window title, the run's status posture, the fallback hint bar.</param>
    /// <param name="log">The transcript the turn writes into and the flux panel displays.</param>
    /// <param name="view">The turn view feeding <paramref name="log"/>; polled for the live tail.</param>
    /// <param name="trajectory">Read (via snapshot) each repaint to fill the steps and detail panels.</param>
    /// <param name="header">Polled each repaint for the clock and token meter.</param>
    /// <param name="body">The turn. Receives a token cancelled when the user asks to stop.</param>
    /// <param name="ct">The caller's token; linked with the surface's own.</param>
    /// <param name="surfaceReady">
    /// Test seam: called with the live application just before the loop starts, so a test can
    /// inject keystrokes. Production passes nothing.
    /// </param>
    /// <param name="onScreen">Test seam: the layout, handed over as soon as it is built.</param>
    public async Task<T> RunTurnAsync<T>(
        TuiChrome chrome,
        TranscriptLog log,
        TuiTurnView view,
        AgentTrajectory trajectory,
        Func<(string Clock, string Meter)> header,
        Func<ITurnView, CancellationToken, Task<T>> body,
        CancellationToken ct,
        Action<IApplication>? surfaceReady = null,
        Action<TuiScreen>? onScreen = null)
    {
        using var stopRequested = new CancellationTokenSource();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, stopRequested.Token);

        var app = Application.Create();
        TuiScreen? screen = null;
        object? repaintToken = null;

        // The engine writes diagnostics straight to the console — an options warning raised inside
        // a tool call, for instance. On the sequential path that is a harmless line; here it would
        // land in the middle of a screen the toolkit believes it owns and corrupt the display.
        // Hold anything written during the session and let it out once the terminal is back.
        var savedOut = Console.Out;
        var savedError = Console.Error;
        var heldOut = new StringWriter();
        var heldError = new StringWriter();

        try
        {
            // Parallel tool calls (F5) can write at the same time, so the console gets a
            // synchronized façade while the buffers stay readable here.
            Console.SetOut(TextWriter.Synchronized(heldOut));
            Console.SetError(TextWriter.Synchronized(heldError));

            if (_driverName is null) app.Init(); else app.Init(_driverName);

            screen = new TuiScreen(chrome);
            onScreen?.Invoke(screen);

            // Raw mode swallows SIGINT: Ctrl+C is a keystroke here and nothing else will turn it
            // back into an interrupt. Cancel the turn and close the surface; the caller maps the
            // resulting OperationCanceledException to exit 130.
            app.Keyboard.KeyDown += (_, key) =>
            {
                if (!TuiKeymap.EndsTheTurn(TuiKeymap.Classify(key))) return;
                key.Handled = true;
                stopRequested.Cancel();
                app.RequestStop();
            };

            long seen = -1;
            repaintToken = app.AddTimeout(RepaintInterval, () =>
            {
                var (clock, meter) = header();
                log.LiveTail = view.LiveTailPlain();

                long version = log.Version;
                bool transcriptChanged = version != seen;
                if (transcriptChanged) seen = version;

                // Title and clock are a cheap string every tick; the panels rebuild only when
                // their source moved (the version counter, the step count, the plan lines).
                screen.Sync(trajectory.Snapshot(),
                    transcriptChanged ? log.PlainLines() : null,
                    clock, meter);
                screen.SyncPlan(view.PlanSnapshot());
                return true;
            });

            // The turn runs off the UI thread; RequestStop in the finally is what lets Run return
            // on every path, including a throw. Queuing it before Run has started is safe — the
            // request survives until the first iteration.
            var turn = Task.Run(async () =>
            {
                try
                {
                    return await body(view, linked.Token);
                }
                finally
                {
                    try { app.Invoke(() => app.RequestStop()); }
                    catch (Exception) { /* the surface is already gone */ }
                }
            }, CancellationToken.None);

            screen.Bind(app);
            surfaceReady?.Invoke(app);
            app.Run(screen.Root);

            return await turn;
        }
        finally
        {
            if (repaintToken is not null)
            {
                try { app.RemoveTimeout(repaintToken); } catch (Exception) { /* already torn down */ }
            }

            try { screen?.Root.Dispose(); } catch (Exception) { /* already torn down */ }
            try { app.Dispose(); } catch (Exception) { /* already torn down */ }

            Console.SetOut(savedOut);
            Console.SetError(savedError);

            // The terminal is ours again — and only now may anything write to it.
            Replay(log);
            Release(heldOut, savedOut);
            Release(heldError, savedError);
        }
    }

    /// <summary>Lets out what the engine wrote while the surface held the screen.</summary>
    private static void Release(StringWriter held, TextWriter destination)
    {
        var text = held.ToString();
        if (!string.IsNullOrEmpty(text))
            destination.Write(text);
    }

    /// <summary>Writes the committed transcript to real scrollback, so the surface leaves a record.</summary>
    private void Replay(TranscriptLog log)
    {
        foreach (var line in log.MarkupLines())
            _console.MarkupLine(line);
        _console.WriteLine();
    }
}

/// <summary>The surface's static labels for one turn.</summary>
/// <param name="Title">Window title — the agent, its model and its mode.</param>
/// <param name="Status">The run's posture, as <see cref="AgentTui.StatusText"/> words it.</param>
/// <param name="Hints">The shortcut bar.</param>
internal readonly record struct TuiChrome(string Title, string Status, string Hints);
