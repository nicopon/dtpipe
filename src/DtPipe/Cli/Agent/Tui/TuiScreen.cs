using System;
using System.Collections.Generic;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui.Panels;
using Terminal.Gui.App;
using Terminal.Gui.Drawing;
using Terminal.Gui.Input;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

namespace DtPipe.Cli.Agent.Tui;

/// <summary>
/// The full-screen layout: a steps list on the left, its detail and the plan/DAG beside it on the
/// right, and along the bottom the exchange — the agent's last word and the line that answers it,
/// in one frame — with a focus-aware hint bar under it. Tab moves between the focusable panels; the
/// detail panel mirrors the steps selection, the plan panel tracks the plan the agent is building.
/// <see cref="Sync"/> and <see cref="SyncPlan"/> are the update points, driven by the repaint timer
/// on the UI thread from thread-safe snapshots.
///
/// <para>
/// The exchange frame is where the session speaks and is spoken to, and its title is where a
/// running turn counts itself — that is the moment it is watched hardest and nothing else on screen
/// is counting. A finished turn reaches it as a projection of <see cref="TurnSummaryModel"/>, the
/// same object the scrollback table renders, so this surface never judges a turn on its own.
/// </para>
/// </summary>
internal sealed class TuiScreen
{
    // The steps panel is a fixed column on the left; the detail panel starts where it ends.
    internal const int StepsWidth = 34;
    // The exchange frame: two border rows, the headline, the agent's words, the input line.
    internal const int ExchangeHeight = 8;
    // rows reserved at the bottom: the exchange frame + the hint bar under it.
    internal const int BottomChrome = ExchangeHeight + 1;
    // The plan panel is the right-hand column, at this share of the screen width.
    internal const int PlanShare = 33;

    private readonly Window _window;
    private readonly StepsPanel _steps = new();
    private readonly PlanPanel _plan = new();
    private readonly DetailPanel _detail;
    private readonly ExchangePanel _exchange = new();
    private readonly Label _hints;
    private readonly Label _caret;
    private readonly TextField _input;
    private readonly int _maxIterations;

    private const string Prompt = "› ";

    /// <summary>How long a passing message holds the hint bar before the shortcuts come back.</summary>
    private const long FlashMs = 1500;

    private bool _expanded;
    private int _working;
    private string _renderedTitle = string.Empty;
    private string _title;
    private Exchange _last;
    private bool _accepting;
    private LiveStep? _live;
    private IApplication? _app;
    private int _copiedStart = -1;
    private int _copiedLength;
    private string? _flash;
    private long _flashUntil;

    /// <summary>A line the user submitted. Raised on the UI thread.</summary>
    public event Action<string>? Submitted;

    /// <param name="chrome">The static labels for the run.</param>
    public TuiScreen(TuiChrome chrome)
    {
        _detail = new DetailPanel(_plan.Frame);
        _title = chrome.Title;
        _last = ExchangeContent.Note(chrome.Status);
        _maxIterations = chrome.MaxIterations;
        _window = new Window { Title = chrome.Title };

        // The caret and the input line live inside the exchange frame: a question and the line that
        // answers it belong to one gesture, and the frame is what says so. They are built before the
        // hint bar, which asks them for the focus while wording itself.
        _caret = new Label { X = 1, Y = Pos.AnchorEnd(1), Width = 2, Text = Prompt };
        _input = new TextField
        {
            X = 4,
            Y = Pos.AnchorEnd(1),
            Width = Dim.Fill(1),
            Height = 1,
            CanFocus = true,
            Visible = true,
            ReadOnly = false,
        };
        _exchange.Frame.Add(_caret, _input);

        _hints = new Label { X = 0, Y = Pos.AnchorEnd(1), Width = Dim.Fill(), Text = HintsFor() };

        _window.Add(_steps.Frame, _detail.Frame, _plan.Frame, _exchange.Frame, _hints);

        _steps.SelectionChanged += RenderDetail;
    }

    public Window Root => _window;

    /// <summary>Wires the focus-aware hint bar and the Tab-between-panels shortcut. Call once.</summary>
    public void Bind(IApplication app)
    {
        var nav = app.Navigation;
        if (nav is not null)
            nav.FocusedChanged += (_, _) =>
            {
                var focused = nav.GetFocused();
                if (_flash is null) _hints.Text = HintsFor();
                MarkFocusedPanel(focused);
            };

        app.Keyboard.KeyDown += (_, key) => OnKey(key, nav?.GetFocused());

        UnpaintTheInputLine();

        // The toolkit builds a text field a right-click menu of its own — select all, cut, undo —
        // on first demand. None of it is this surface's vocabulary, and its own property is
        // read-only, so the demand is what gets refused. Selecting with the mouse copies instead.
        _input.MouseEvent += (_, mouse) =>
        {
            if (mouse.Flags.HasFlag(MouseFlags.RightButtonPressed)
                || mouse.Flags.HasFlag(MouseFlags.RightButtonReleased)
                || mouse.Flags.HasFlag(MouseFlags.RightButtonClicked))
                mouse.Handled = true;
        };

        _app = app;
        _input.SetFocus();
    }

    /// <summary>
    /// Makes the input line paint like every panel around it. The scheme is read back from the
    /// view, so whichever theme is in force is the one inherited.
    ///
    /// <para>
    /// Every other view on this surface draws with the scheme's <c>Normal</c> attribute, which is
    /// fully inherited — that is why the layout takes the user's own terminal colours. A text field
    /// draws with <c>Editable</c>, <c>ReadOnly</c> and <c>Focus</c> instead, and in the stock scheme
    /// those are white-on-grey, grey-on-grey and black-on-white. Two of the three are unreadable
    /// here: the line spans the full width, so <c>Focus</c> lays a solid band across the screen for
    /// the whole time between turns, and <c>ReadOnly</c> — grey text on a grey ground — is what
    /// <see cref="SetAccepting"/> selects for the whole of every turn. The open / closed state is
    /// carried by the caret (<c>›</c> / <c>⏳</c>), which no theme can wash out.
    /// </para>
    /// </summary>
    private void UnpaintTheInputLine()
    {
        var inherited = _input.GetScheme();
        _input.SetScheme(new Scheme(inherited)
        {
            Editable = inherited.Normal,
            ReadOnly = inherited.Normal,
            Focus = inherited.Normal,
        });
    }

    /// <summary>
    /// Opens or closes the input line. While a turn runs the line is read-only and says so: a
    /// prompt typed mid-turn would either be dropped silently or race the conversation the model
    /// is still building.
    /// </summary>
    public void SetAccepting(bool accepting)
    {
        _accepting = accepting;
        _input.ReadOnly = !accepting;
        _working = 0;
        _caret.Text = accepting ? Prompt : $"{Wheel.Frame(0)} ";
        if (accepting) _input.SetFocus();
    }

    /// <summary>
    /// Puts the step list back on the running row. A new prompt is a deliberate change of subject:
    /// whatever earlier step was being read, the user has just asked for something else and wants to
    /// see it happen.
    /// </summary>
    public void FollowTheTurn() => _steps.SelectLast();

    /// <summary>The session answering for itself — what a command did, or why it did nothing.</summary>
    public void ShowNote(string line) => _last = ExchangeContent.Note(line);

    /// <summary>
    /// A finished turn, as the exchange shows it. The surface never judges a turn: the marker and
    /// the headline are derived from <paramref name="summary"/>, which the executor built and the
    /// scrollback table renders from too.
    /// </summary>
    public void ShowTurn(TurnSummaryModel summary, bool hasPlan) => _last = ExchangeContent.Of(summary, hasPlan);

    /// <summary>Re-labels the run — the operating mode is in the title, and it can change mid-session.</summary>
    public void Rechrome(string title) => _title = title;

    /// <summary>
    /// Pushes the latest state into the panels. The snapshots are already detached copies; this
    /// only touches views, on the UI thread. <paramref name="live"/> is the step the model is
    /// producing, or null between steps.
    ///
    /// <para>
    /// Every write here is conditional, because the repaint timer runs ten times a second for the
    /// whole session while most of what it reads is standing still: between turns the clock is
    /// stopped and the token count is final, so the title and the status band would otherwise be
    /// handed the very same string ten times a second with nothing on screen having changed. The
    /// panels each guard themselves the same way.
    /// </para>
    /// </summary>
    public void Sync(IReadOnlyList<TrajectoryStep> steps, LiveStep? live, string clock, string meter)
    {
        if (_title != _renderedTitle) _window.Title = _renderedTitle = _title;

        // The exchange holds the last word said to the user, and while a turn runs it says only
        // that one is running — what the model is producing belongs to the step producing it, over
        // in the detail panel. Its frame title does the counting.
        _exchange.Retitle(_accepting ? "Agent" : $"Agent · {Progress(steps.Count, clock, meter)}");
        _exchange.Show(_accepting ? _last : ExchangeContent.Working());

        // The one thing on this surface that redraws with nothing behind it having changed, and
        // that is its whole job: it says the model is still working. The steps list spins the same
        // frame on the running row, so the two turn together.
        if (!_accepting)
        {
            _working++;
            _caret.Text = $"{Wheel.Frame(_working)} ";
        }

        _live = live;
        _steps.Update(steps, live, Wheel.Frame(_working));
        RenderDetail();
        CopyWhatWasSelected();
        ExpireFlash();
    }

    /// <summary>
    /// Puts a selection made on the input line straight into the clipboard. Polled rather than
    /// hooked: a selection grows through a drag, a shift-arrow and a double click alike, and this
    /// way the last state of any of them is what lands there. Silent when the terminal has no
    /// clipboard — the surface says nothing it cannot deliver.
    /// </summary>
    private void CopyWhatWasSelected()
    {
        int length = _input.SelectedLength;
        if (length <= 0) { _copiedLength = 0; return; }

        int start = _input.SelectedStart;
        if (start == _copiedStart && length == _copiedLength) return;
        _copiedStart = start;
        _copiedLength = length;

        var text = _input.SelectedText;
        if (string.IsNullOrEmpty(text)) return;
        try
        {
            if (_app?.Clipboard?.TrySetClipboardData(text) == true) Flash("copied to the clipboard");
        }
        catch (Exception)
        {
            // No clipboard on this terminal; the selection still stands.
        }
    }

    /// <summary>Says something for a moment, on the line that is otherwise the shortcuts.</summary>
    private void Flash(string line)
    {
        _flash = line;
        _flashUntil = Environment.TickCount64 + FlashMs;
        _hints.Text = line;
    }

    private void ExpireFlash()
    {
        if (_flash is null || Environment.TickCount64 < _flashUntil) return;
        _flash = null;
        _hints.Text = HintsFor();
    }

    /// <summary>
    /// How far the running turn has got. The step count is the trajectory's own length, so it needs
    /// nothing the surface is not already handed every tick; the ceiling is the run's, fixed at
    /// launch. The wheel is on the caret, not here — one turn is one moving thing.
    /// </summary>
    private string Progress(int stepsTaken, string clock, string meter)
    {
        var line = $"step {stepsTaken + 1}/{_maxIterations} · {clock}";
        return meter.Length > 0 ? $"{line} · {meter}" : line;
    }

    /// <summary>Pushes the plan's latest state into the plan panel. A no-op when nothing changed.</summary>
    public void SyncPlan(PlanView plan) => _plan.Update(plan);

    /// <summary>
    /// Re-renders the detail — the running step's live text when that row is highlighted, the
    /// recorded step otherwise — and hands it the plan's column while it is expanded. The plan stands
    /// down rather than being squeezed: an expanded step detail is the one thing here that wants
    /// the whole width, and a branch line rendered into half a column says nothing.
    /// </summary>
    private void RenderDetail()
    {
        if (_steps.LiveSelected && _live is { } live) _detail.ShowLive(live);
        else _detail.Show(_steps.Selected, _expanded);

        _plan.Frame.Visible = !_expanded;
        _detail.Widen(_expanded);
    }

    /// <summary>
    /// Shortcuts handled above the panels, every one of them named by <see cref="TuiKeymap"/> — this
    /// method reads signals, never a raw key. <see cref="SurfaceSignal.FocusNext"/> moves around the
    /// focus ring from anywhere (a FrameView is a tab *group*, so its own Tab stays inside — this
    /// flattens them). On the input line only <see cref="SurfaceSignal.Submit"/> is a shortcut;
    /// every other key is typing. While the steps panel has focus <c>e</c>/<c>b</c> jump between
    /// errors and Right/Enter/Left expand or collapse the detail. Up/Down are left for the list.
    /// </summary>
    private void OnKey(Key key, View? focused)
    {
        // The views that matter are nested now, and the navigator reports the focused branch rather
        // than its leaf — so ask each view whether it holds the focus instead of matching an
        // identity the navigator never promised to hand back.
        bool onInput = _input.HasFocus;
        var signal = TuiKeymap.Classify(key);

        if (signal == SurfaceSignal.FocusNext)
        {
            key.Handled = true;
            NextInRing(focused).SetFocus();
            return;
        }

        if (onInput)
        {
            if (signal != SurfaceSignal.Submit) return;   // everything else is typing
            key.Handled = true;
            if (!_accepting) return;
            var line = _input.Text ?? string.Empty;
            _input.Text = string.Empty;
            Submitted?.Invoke(line);
            return;
        }

        // Error navigation moves the steps selection, so it belongs to the steps panel. Expanding
        // is about the detail, and the reader who wants it is as likely to be standing in the
        // detail as in the list.
        if (Owns(_steps.Frame, focused))
        {
            switch (signal)
            {
                case SurfaceSignal.NextError: _steps.JumpToError(+1); key.Handled = true; return;
                case SurfaceSignal.PrevError: _steps.JumpToError(-1); key.Handled = true; return;
            }
        }

        if (!Owns(_steps.Frame, focused) && !_detail.FocusTarget.HasFocus) return;

        switch (signal)
        {
            case SurfaceSignal.Expand or SurfaceSignal.Submit:
                _expanded = true; RenderDetail(); key.Handled = true; return;
            case SurfaceSignal.Collapse:
                _expanded = false; RenderDetail(); key.Handled = true; return;
        }
    }

    /// <summary>
    /// Thickens the border of whichever panel holds the focus. The weight of a line is the one
    /// emphasis that needs no colour, so it survives any terminal theme — this surface paints
    /// nothing and inherits everything. A panel that cannot take focus simply never thickens.
    /// </summary>
    private void MarkFocusedPanel(View? focused)
    {
        foreach (var frame in new[] { _steps.Frame, _detail.Frame, _plan.Frame, _exchange.Frame })
            if (frame.Border is { } border)
                border.LineStyle = Owns(frame, focused) ? LineStyle.Heavy : LineStyle.Rounded;
    }

    /// <summary>
    /// Steps → Detail → Plan → the agent's words → the input line → Steps. A hidden plan is skipped:
    /// the detail takes its column while expanded, and handing the focus to a view nobody can see
    /// would look like the ring had simply stopped.
    /// </summary>
    private View NextInRing(View? focused)
    {
        if (Owns(_steps.Frame, focused)) return _detail.FocusTarget;
        if (_detail.FocusTarget.HasFocus) return _plan.Frame.Visible ? _plan.FocusTarget : _exchange.FocusTarget;
        if (_plan.FocusTarget.HasFocus) return _exchange.FocusTarget;
        if (_exchange.FocusTarget.HasFocus) return _input;
        return _steps.FocusTarget;
    }

    // ── test inspection ────────────────────────────────────────────────────────
    internal string HintsText => _hints.Text;
    internal string StatusText => $"{_exchange.HeadlineText}\n{_exchange.BodyText}";
    internal bool Accepting => _accepting;
    internal string InputText { get => _input.Text ?? string.Empty; set => _input.Text = value; }
    internal Scheme InputScheme => _input.GetScheme();
    internal string CaretText => _caret.Text;
    internal string WindowTitle => _window.Title;
    internal string ExchangeTitle => _exchange.Frame.Title;
    internal bool InputFocused => _input.HasFocus;
    internal bool LiveStepSelected => _steps.LiveSelected;
    internal int? ExchangeSelected => _exchange.SelectedIndex;
    internal bool PlanVisible => _plan.Frame.Visible;
    internal int PlanWidth => _plan.Frame.Frame.Width;
    internal int PlanLeft => _plan.Frame.Frame.X;
    internal int DetailRight => _detail.Frame.Frame.X + _detail.Frame.Frame.Width;
    internal LineStyle? BorderOfSteps => _steps.Frame.Border?.LineStyle;
    internal LineStyle? BorderOfExchange => _exchange.Frame.Border?.LineStyle;
    internal string DetailText => _detail.BodyText;
    internal string DetailTitle => _detail.Frame.Title;
    internal string PlanText => _plan.BodyText;
    internal bool Expanded => _expanded;
    internal int? SelectedStepIteration => _steps.Selected?.Iteration;
    internal void FocusExchange() => _exchange.FocusTarget.SetFocus();
    internal void FocusSteps() => _steps.FocusTarget.SetFocus();
    internal void FocusPlan() => _plan.FocusTarget.SetFocus();

    private string HintsFor()
    {
        if (_input.HasFocus)
            return $"enter sends · {SessionCommand.Hint} · esc stop · ^C quit";

        return _steps.FocusTarget.HasFocus ? "↑↓ select · e/b errors · →/enter expand · ⇥ panel · esc stop"
             : _detail.FocusTarget.HasFocus ? "↑↓ scroll · →/← expand · ⇥ panel · esc stop"
             : _plan.FocusTarget.HasFocus ? "↑↓ scroll the plan · ⇥ panel · esc stop"
             : _exchange.FocusTarget.HasFocus ? "↑↓ scroll the agent's words · ⇥ panel · esc stop"
             : "⇥ panel · esc stop · ^C quit";
    }

    private static bool Owns(View container, View? candidate)
    {
        for (var v = candidate; v is not null; v = v.SuperView)
            if (ReferenceEquals(v, container))
                return true;
        return false;
    }
}
