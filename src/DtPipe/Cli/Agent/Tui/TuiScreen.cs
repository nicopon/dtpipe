using System;
using System.Collections.Generic;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui.Panels;
using Terminal.Gui.App;
using Terminal.Gui.Drivers;
using Terminal.Gui.Input;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

namespace DtPipe.Cli.Agent.Tui;

/// <summary>
/// The full-screen layout: a steps list on the left, its detail and the plan/DAG stacked on the
/// right, the running transcript in a band below, then a status line, a focus-aware hint bar and
/// the input line. Tab moves between the focusable panels; the detail panel mirrors the steps
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

    private bool _expanded;
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
        _hints = new Label { X = 0, Y = Pos.AnchorEnd(2), Width = Dim.Fill(), Text = HintsFor(null) };
        _caret = new Label { X = 0, Y = Pos.AnchorEnd(1), Width = 2, Text = "› " };
        _input = new TextField
        {
            X = 2,
            Y = Pos.AnchorEnd(1),
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
            nav.FocusedChanged += (_, _) => _hints.Text = HintsFor(nav.GetFocused());

        app.Keyboard.KeyDown += (_, key) => OnKey(key, nav?.GetFocused());

        _input.SetFocus();
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
        _caret.Text = accepting ? "› " : "⏳ ";
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
    /// </summary>
    public void Sync(IReadOnlyList<TrajectoryStep> steps, IReadOnlyList<string>? fluxLines, string clock, string meter)
    {
        _window.Title = meter.Length > 0 ? $"{_title}    {clock} · {meter}" : $"{_title}    {clock}";
        _status.Text = _verdict ?? _posture;
        _steps.Update(steps);            // rebuilds only when the step count moved
        if (fluxLines is not null) _flux.Update(fluxLines);
    }

    /// <summary>Pushes the plan's latest state into the plan panel. A no-op when nothing changed.</summary>
    public void SyncPlan(PlanView plan) => _plan.Update(plan);

    private void RenderDetail() => _detail.Show(_steps.Selected, _expanded);

    /// <summary>
    /// Shortcuts handled above the panels: plain Tab moves around the focus ring (a FrameView is a
    /// tab *group*, so its own Tab stays inside — this flattens them), Enter on the input line
    /// submits, and while the steps panel has focus <c>e</c>/<c>b</c> jump between errors and
    /// Right/Enter/Left expand or collapse the detail. Up/Down are left for the list itself.
    /// </summary>
    private void OnKey(Key key, View? focused)
    {
        bool onInput = ReferenceEquals(focused, _input);

        if (key.KeyCode == KeyCode.Tab)
        {
            key.Handled = true;
            NextInRing(focused).SetFocus();
            return;
        }

        if (onInput)
        {
            if (key.KeyCode != KeyCode.Enter) return;   // everything else is typing
            key.Handled = true;
            if (!_accepting) return;
            var line = _input.Text ?? string.Empty;
            _input.Text = string.Empty;
            Submitted?.Invoke(line);
            return;
        }

        if (!Owns(_steps.Frame, focused)) return;

        switch (key.AsRune.Value)
        {
            case 'e': _steps.JumpToError(+1); key.Handled = true; return;
            case 'b': _steps.JumpToError(-1); key.Handled = true; return;
        }

        switch (key.KeyCode)
        {
            case KeyCode.CursorRight or KeyCode.Enter:
                _expanded = true; RenderDetail(); key.Handled = true; return;
            case KeyCode.CursorLeft:
                _expanded = false; RenderDetail(); key.Handled = true; return;
        }
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
