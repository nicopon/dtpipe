using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Spectre.Console;
using Terminal.Gui.App;
using Terminal.Gui.Views;

namespace DtPipe.Cli.Agent.Tui;

/// <summary>
/// Runs the agent inside a Terminal.Gui application that owns the screen, then hands the terminal
/// back and replays the transcript to real scrollback. The full-screen surface is a presentation;
/// the permanent record stays the scrollback lines, and the verdict stays the projection of a
/// <see cref="TurnSummaryModel"/> the executor built.
///
/// <para>
/// <see cref="RunSessionAsync"/> holds the application for a whole conversation — several turns,
/// an input line between them — over one create/run/teardown lifecycle.
/// </para>
///
/// <para>
/// Three rules hold this together and all three are easy to break:
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
/// <item>
/// <b>Two cancellation sources, never one.</b> Ctrl+C ends the session and the caller reports 130;
/// Esc cancels the model call and the session lives. Feeding Esc into the process token would make
/// a soft stop indistinguishable from a real interrupt (F16).
/// </item>
/// </list>
/// </summary>
internal sealed class TuiApp
{
    private static readonly TimeSpan RepaintInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Character ceiling on each held console buffer. A real run's engine diagnostics are a handful
    /// of lines; past a megabyte the run is flooding the quarantine, and only the tail — the last
    /// error before exit — is worth holding in memory for the whole session.
    /// </summary>
    private const int QuarantineCap = 1024 * 1024;

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
    /// Holds the surface for a whole conversation. <paramref name="session"/> drives it from a
    /// worker thread: it runs turns, reads the input line between them, and returns when the user
    /// leaves. The worker's exception — including the cancellation Ctrl+C raises — propagates to
    /// the caller after the terminal has been restored and the transcript replayed, so an
    /// interrupted run still reports 130 and still leaves its partial record behind.
    /// </summary>
    /// <param name="chrome">The window title and the run's status posture.</param>
    /// <param name="log">The transcript the turns write into and the flux panel displays.</param>
    /// <param name="view">The turn view feeding <paramref name="log"/>; polled for the live tail.</param>
    /// <param name="trajectory">Read (via snapshot) each repaint to fill the steps and detail panels.</param>
    /// <param name="header">Polled each repaint for the clock and token meter.</param>
    /// <param name="session">The conversation. Runs on a worker thread and drives the surface through <see cref="TuiSurface"/>.</param>
    /// <param name="ct">The caller's token; linked with the surface's own.</param>
    /// <param name="surfaceReady">
    /// Test seam: called with the live application just before the loop starts, so a test can
    /// inject keystrokes. Production passes nothing.
    /// </param>
    /// <param name="onScreen">Test seam: the layout, handed over as soon as it is built.</param>
    public async Task RunSessionAsync(
        TuiChrome chrome,
        TranscriptLog log,
        TuiTurnView view,
        AgentTrajectory trajectory,
        Func<(string Clock, string Meter)> header,
        Func<TuiSurface, Task> session,
        CancellationToken ct,
        Action<IApplication>? surfaceReady = null,
        Action<TuiScreen>? onScreen = null)
    {
        using var stopRequested = new CancellationTokenSource();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, stopRequested.Token);

        var app = Application.Create();
        TuiScreen? screen = null;
        object? repaintToken = null;

        // Two things here outlive any single turn and reach a reader only once the terminal is
        // back: this console quarantine and the transcript. The session owns the screen across
        // every turn of the conversation, and nothing may write through Spectre while the toolkit
        // owns it — the teardown ordering in the finally is that contract. So an engine diagnostic
        // written mid-turn, an options warning raised inside a tool call for instance, is held
        // rather than printed until Dispose has handed the terminal back; it cannot be released
        // turn by turn.
        //
        // The transcript is the run's record and is replayed whole. These two buffers are not a
        // record — they are diagnostics nobody asked for — so each is bounded and keeps only its
        // tail, with a notice standing in for whatever it had to drop.
        var savedOut = Console.Out;
        var savedError = Console.Error;
        var heldOut = new BoundedTailWriter(QuarantineCap);
        var heldError = new BoundedTailWriter(QuarantineCap);

        try
        {
            // BoundedTailWriter synchronises its own writes, so the buffers go in bare and stay
            // readable here for the drain at teardown.
            Console.SetOut(heldOut);
            Console.SetError(heldError);

            if (_driverName is null) app.Init(); else app.Init(_driverName);

            screen = new TuiScreen(chrome);
            onScreen?.Invoke(screen);

            var surface = new TuiSurface(app, screen, linked.Token);
            screen.Submitted += surface.OfferLine;

            app.Keyboard.KeyDown += (_, key) =>
            {
                var signal = TuiKeymap.Classify(key);

                // Raw mode swallows SIGINT: Ctrl+C is a keystroke here and nothing else will turn
                // it back into an interrupt. Cancel everything and close the surface; the caller
                // maps the resulting OperationCanceledException to exit 130.
                if (TuiKeymap.EndsTheSession(signal))
                {
                    key.Handled = true;
                    stopRequested.Cancel();
                    app.RequestStop();
                    return;
                }

                // Esc is the toolkit's own quit key. Leaving it unhandled tears the application
                // down instead of cancelling one model call.
                if (signal == SurfaceSignal.Interrupt)
                {
                    key.Handled = true;
                    surface.RequestSoftCancel();
                    return;
                }

                // Shift+Tab would otherwise walk the focus ring backwards. The mode cycle goes
                // through the command path, which drops it while a turn is running (F1).
                if (signal == SurfaceSignal.CycleMode)
                {
                    key.Handled = true;
                    surface.OfferLine("/mode");
                }
            };

            repaintToken = app.AddTimeout(RepaintInterval, () =>
            {
                var (clock, meter) = header();
                log.LiveTail = view.LiveTailPlain();

                // The panels rebuild only when their source moved (the step count, the rendered
                // exchange, the plan lines); the transcript keeps the permanent record and is
                // replayed to scrollback at teardown, not shown here line by line.
                screen.Sync(trajectory.Snapshot(), view.LiveStepSnapshot(), clock, meter);
                screen.SyncPlan(view.PlanSnapshot());
                return true;
            });

            // The session runs off the UI thread; RequestStop in the finally is what lets Run
            // return on every path, including a throw. Queuing it before Run has started is safe —
            // the request survives until the first iteration.
            var worker = Task.Run(async () =>
            {
                try
                {
                    await session(surface);
                }
                finally
                {
                    try { app.Invoke(() => app.RequestStop()); }
                    catch (Exception) { /* the surface is already gone */ }
                }
            }, CancellationToken.None);

            screen.Bind(app);
            screen.SetAccepting(true);
            surfaceReady?.Invoke(app);
            app.Run(screen.Root);

            await worker;
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

    /// <summary>Lets out what the engine wrote while the surface held the screen — tail first,
    /// behind a notice, when the buffer overflowed its cap.</summary>
    private static void Release(BoundedTailWriter held, TextWriter destination)
    {
        var text = held.Drain();
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

    /// <summary>
    /// A <see cref="TextWriter"/> that retains only the last <c>capacity</c> characters and counts
    /// what it dropped. The session quarantine holds every engine write until the terminal is
    /// handed back; unbounded, a flooding run would keep it all in memory and pour it out at once.
    /// Overflow is discarded from the front — the tail carries the last error before exit — and
    /// <see cref="Drain"/> puts a notice line where the rest was. Synchronised: parallel tool
    /// calls (F5) can write at the same moment.
    /// </summary>
    private sealed class BoundedTailWriter : TextWriter
    {
        private readonly int _capacity;
        private readonly object _gate = new();
        private readonly StringBuilder _buffer = new();
        private long _dropped;

        public BoundedTailWriter(int capacity) => _capacity = capacity;

        public override Encoding Encoding => Encoding.UTF8;

        public override void Write(char value)
        {
            lock (_gate) { _buffer.Append(value); TrimLocked(); }
        }

        public override void Write(string? value)
        {
            if (string.IsNullOrEmpty(value)) return;
            lock (_gate) { _buffer.Append(value); TrimLocked(); }
        }

        public override void Write(char[] buffer, int index, int count)
        {
            lock (_gate) { _buffer.Append(buffer, index, count); TrimLocked(); }
        }

        public override void Write(ReadOnlySpan<char> buffer)
        {
            lock (_gate) { _buffer.Append(buffer); TrimLocked(); }
        }

        /// <summary>The retained tail, with a notice line ahead of it when earlier output was cut.</summary>
        public string Drain()
        {
            lock (_gate)
            {
                if (_dropped == 0) return _buffer.ToString();
                long kb = (_dropped + 1023) / 1024;
                return $"[dtpipe] {kb} KB of earlier engine output omitted{Environment.NewLine}{_buffer}";
            }
        }

        // Trim lazily: let the buffer reach twice the cap, then drop back to it. That is one
        // O(cap) shift per cap characters written — amortised O(1) — where trimming on every
        // write would be O(n) each time and choke on a real flood.
        private void TrimLocked()
        {
            if (_buffer.Length <= _capacity * 2) return;
            int excess = _buffer.Length - _capacity;
            _buffer.Remove(0, excess);
            _dropped += excess;
        }
    }
}

/// <summary>
/// What a session's worker thread may do to the live surface: read the input line, run a turn
/// under a cancellation it alone owns, and marshal work onto the UI thread. Everything here is
/// called from the worker except <see cref="OfferLine"/> and <see cref="RequestSoftCancel"/>,
/// which the key handler raises on the UI thread.
/// </summary>
internal sealed class TuiSurface
{
    private readonly IApplication _app;
    private readonly Channel<string> _lines = Channel.CreateUnbounded<string>();
    private readonly object _gate = new();

    private CancellationTokenSource? _softCancel;

    internal TuiSurface(IApplication app, TuiScreen screen, CancellationToken sessionToken)
    {
        _app = app;
        Screen = screen;
        SessionToken = sessionToken;
    }

    /// <summary>Cancelled when the user leaves (Ctrl+C) or the caller's own token trips.</summary>
    public CancellationToken SessionToken { get; }

    public TuiScreen Screen { get; }

    /// <summary>True while a turn is running — the input line is closed and drops what it is given.</summary>
    public bool TurnInFlight { get { lock (_gate) return _softCancel is not null; } }

    /// <summary>UI thread: a submitted line, or a shortcut that spells one. Dropped mid-turn.</summary>
    internal void OfferLine(string line)
    {
        if (TurnInFlight) return;
        _lines.Writer.TryWrite(line);
    }

    /// <summary>UI thread: Esc. Cancels the model call in flight and nothing else.</summary>
    internal void RequestSoftCancel()
    {
        lock (_gate)
        {
            try { _softCancel?.Cancel(); }
            catch (ObjectDisposedException) { /* the turn ended between the keypress and here */ }
        }
    }

    /// <summary>Waits for the next line the user submits. Throws when the session ends.</summary>
    public async Task<string> ReadLineAsync() => await _lines.Reader.ReadAsync(SessionToken);

    /// <summary>
    /// Arms the soft cancel for one turn and closes the input line. The returned token is the one
    /// Esc trips; it is never the session token, so a soft stop cannot be read as an interrupt.
    /// </summary>
    public CancellationToken BeginTurn()
    {
        CancellationTokenSource cts;
        lock (_gate)
        {
            _softCancel = cts = new CancellationTokenSource();
        }
        Post(() => { Screen.SetAccepting(false); Screen.FollowTheTurn(); });
        return cts.Token;
    }

    /// <summary>Disarms the soft cancel and reopens the input line.</summary>
    public void EndTurn()
    {
        CancellationTokenSource? cts;
        lock (_gate)
        {
            cts = _softCancel;
            _softCancel = null;
        }
        cts?.Dispose();
        Post(() => Screen.SetAccepting(true));
    }

    /// <summary>Runs <paramref name="action"/> on the UI thread. Every view mutation goes through here.</summary>
    public void Post(Action action)
    {
        try { _app.Invoke(action); }
        catch (Exception) { /* the surface is already gone */ }
    }

    /// <summary>
    /// The last gate before a real write, as a modal dialog. The scrollback path keeps its Spectre
    /// confirmation; a prompt written through Spectre here would land on a screen the toolkit owns.
    /// </summary>
    public Task<bool> ConfirmAsync(string title, string message)
    {
        var answer = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Post(() =>
        {
            try
            {
                // The last button is the default one, so "Execute" must not be it: this gate exists
                // to make a real write a deliberate act.
                int? choice = MessageBox.Query(_app, title, message, "Execute", "Cancel");
                answer.TrySetResult(choice == 0);
            }
            catch (Exception)
            {
                answer.TrySetResult(false);   // fail closed: no dialog, no write
            }
        });
        return answer.Task;
    }
}

/// <summary>The surface's static labels for one run. The hint bar is not here — it is
/// focus-dependent and built by <see cref="TuiScreen.HintsFor"/>.</summary>
/// <param name="Title">Window title — the agent, its model and its mode.</param>
/// <param name="Status">The run's posture, as <see cref="AgentTui.StatusText"/> words it.</param>
/// <param name="MaxIterations">The turn's iteration ceiling, the denominator of the step count.</param>
internal readonly record struct TuiChrome(string Title, string Status, int MaxIterations = 25);
