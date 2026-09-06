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
/// The full-screen layout: a steps list on the left, its detail and the plan/DAG stacked on the
/// right, the running transcript in a band below, then a status line, the input line, and a
/// focus-aware hint bar along the bottom edge — the hints stay under the line they describe.
/// Tab moves between the focusable panels; the detail panel mirrors the steps
/// selection, the plan panel tracks the plan the agent is building. <see cref="Sync"/> and
/// <see cref="SyncPlan"/> are the update points, driven by the repaint timer on the UI thread
/// from thread-safe snapshots.
///
/// <para>
/// The status line carries two things at different times: the run's posture while a turn is in
/// flight, and the finished turn's verdict afterwards. The verdict is a projection of
/// <see cref="TurnSummaryModel"/> — the same object the scrollback table renders — so this surface
/// never judges a turn on its own.
/// </para>
/// </summary>
internal sealed class TuiScreen
{
    // The steps panel is a fixed column on the left; the detail panel starts where it ends.
    internal const int StepsWidth = 34;
    internal const int FluxHeight = 6;
    // rows reserved at the bottom: the flux band + the status line + the hint bar + the input line.
    internal const int BottomChrome = FluxHeight + 3;
    // The plan panel splits the right column: detail on top, plan (this many rows) below it.
    internal const int PlanHeight = 7;

    private readonly Window _window;
    private readonly StepsPanel _steps = new();
    private readonly DetailPanel _detail = new();
    private readonly PlanPanel _plan = new();
    private readonly FluxPanel _flux = new();
    private readonly Label _status;
    private readonly Label _hints;
    private readonly Label _caret;
    private readonly TextField _input;

    /// <summary>
    /// The frames of a turn in flight, on the caret. Braille dots, the same wheel the scrollback
    /// path spins through Spectre — one turn, one vocabulary, whichever surface is watching.
    /// </summary>
    private static readonly string[] Working =
        ["⠋ ", "⠙ ", "⠹ ", "⠸ ", "⠼ ", "⠴ ", "⠦ ", "⠧ ", "⠇ ", "⠏ "];

    private const string Prompt = "› ";

    private bool _expanded;
    private int _working;
    private string _renderedTitle = string.Empty;
    private string _renderedStatus = string.Empty;
    private string _title;
    private string _posture;
    private string? _verdict;
    private bool _accepting;

    /// <summary>A line the user submitted. Raised on the UI thread.</summary>
    public event Action<string>? Submitted;

    /// <param name="chrome">The static labels for the run.</param>
    public TuiScreen(TuiChrome chrome)
    {
        _title = chrome.Title;
        _posture = chrome.Status;
        _window = new Window { Title = chrome.Title };

        _status = new Label { X = 0, Y = Pos.AnchorEnd(3), Width = Dim.Fill(), Text = chrome.Status };
        _caret = new Label { X = 0, Y = Pos.AnchorEnd(2), Width = 2, Text = Prompt };
        _hints = new Label { X = 0, Y = Pos.AnchorEnd(1), Width = Dim.Fill(), Text = HintsFor(null) };
        _input = new TextField
        {
            X = 2,
            Y = Pos.AnchorEnd(2),
            Width = Dim.Fill(),
            Height = 1,
            CanFocus = true,
            Visible = true,
            ReadOnly = false,
        };

        _window.Add(_steps.Frame, _detail.Frame, _plan.Frame, _flux.Frame, _status, _hints, _caret, _input);

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
                _hints.Text = HintsFor(focused);
                MarkFocusedPanel(focused);
            };

        app.Keyboard.KeyDown += (_, key) => OnKey(key, nav?.GetFocused());

        UnpaintTheInputLine();
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
        _caret.Text = accepting ? Prompt : Working[0];
        if (accepting) _input.SetFocus();
    }

    /// <summary>
    /// Writes one line into the status band: a finished turn's verdict, or a session note such as
    /// what a command did. Null restores the run's posture. It never carries a judgement of its
    /// own — a verdict reaches here as <see cref="TurnSummaryModel.VerdictLine"/>.
    /// </summary>
    public void ShowStatus(string? line) => _verdict = line;

    /// <summary>Re-labels the run — the operating mode shows in both, and it can change mid-session.</summary>
    public void Rechrome(string title, string posture)
    {
        _title = title;
        _posture = posture;
    }

    /// <summary>
    /// Pushes the latest state into the panels. The snapshots are already detached copies; this
    /// only touches views, on the UI thread. <paramref name="fluxLines"/> is null when the
    /// transcript has not moved since the last tick, so the flux list is left alone.
    ///
    /// <para>
    /// Every write here is conditional, because the repaint timer runs ten times a second for the
    /// whole session while most of what it reads is standing still: between turns the clock is
    /// stopped and the token count is final, so the title and the status band would otherwise be
    /// handed the very same string ten times a second with nothing on screen having changed. The
    /// panels each guard themselves the same way.
    /// </para>
    /// </summary>
    public void Sync(IReadOnlyList<TrajectoryStep> steps, IReadOnlyList<string>? fluxLines, string clock, string meter)
    {
        var title = meter.Length > 0 ? $"{_title}    {clock} · {meter}" : $"{_title}    {clock}";
        if (title != _renderedTitle) _window.Title = _renderedTitle = title;

        var status = _verdict ?? _posture;
        if (status != _renderedStatus) _status.Text = _renderedStatus = status;

        // The one thing on this surface that redraws with nothing behind it having changed, and
        // that is its whole job: it says the model is still working. Two cells, and only while a
        // turn holds the line closed.
        if (!_accepting)
        {
            _working = (_working + 1) % Working.Length;
            _caret.Text = Working[_working];
        }

        _steps.Update(steps);            // rebuilds only when the step count moved
        if (fluxLines is not null) _flux.Update(fluxLines);
    }

    /// <summary>Pushes the plan's latest state into the plan panel. A no-op when nothing changed.</summary>
    public void SyncPlan(PlanView plan) => _plan.Update(plan);

    private void RenderDetail() => _detail.Show(_steps.Selected, _expanded);

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
        bool onInput = ReferenceEquals(focused, _input);
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

        if (!Owns(_steps.Frame, focused)) return;

        switch (signal)
        {
            case SurfaceSignal.NextError: _steps.JumpToError(+1); key.Handled = true; return;
            case SurfaceSignal.PrevError: _steps.JumpToError(-1); key.Handled = true; return;
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
        foreach (var frame in new[] { _steps.Frame, _detail.Frame, _plan.Frame, _flux.Frame })
            if (frame.Border is { } border)
                border.LineStyle = Owns(frame, focused) ? LineStyle.Heavy : LineStyle.Rounded;
    }

    /// <summary>Steps → Flux → input → Steps.</summary>
    private View NextInRing(View? focused)
    {
        if (Owns(_steps.Frame, focused)) return _flux.FocusTarget;
        if (Owns(_flux.Frame, focused)) return _input;
        return _steps.FocusTarget;
    }

    // ── test inspection ────────────────────────────────────────────────────────
    internal string HintsText => _hints.Text;
    internal string StatusText => _status.Text;
    internal bool Accepting => _accepting;
    internal string InputText { get => _input.Text ?? string.Empty; set => _input.Text = value; }
    internal Scheme InputScheme => _input.GetScheme();
    internal string CaretText => _caret.Text;
    internal LineStyle? BorderOfSteps => _steps.Frame.Border?.LineStyle;
    internal LineStyle? BorderOfFlux => _flux.Frame.Border?.LineStyle;
    internal string DetailText => _detail.BodyText;
    internal string PlanText => _plan.BodyText;
    internal bool Expanded => _expanded;
    internal int? SelectedStepIteration => _steps.Selected?.Iteration;
    internal void FocusFlux() => _flux.FocusTarget.SetFocus();
    internal void FocusSteps() => _steps.FocusTarget.SetFocus();

    private string HintsFor(View? focused)
    {
        if (ReferenceEquals(focused, _input))
            return $"enter run · {SessionCommand.Hint} · esc stop · ^C quit";

        bool onSteps = Owns(_steps.Frame, focused);
        bool onFlux = Owns(_flux.Frame, focused);
        return onSteps ? "↑↓ select · e/b errors · →/enter expand · ⇥ panel · esc stop"
             : onFlux ? "↑↓ scroll · ⇥ panel · esc stop"
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
