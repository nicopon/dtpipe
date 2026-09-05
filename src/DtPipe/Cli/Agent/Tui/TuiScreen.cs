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
/// The full-screen layout: a steps list and its detail across the top, the running transcript in a
/// band below, a status line and a focus-aware hint bar at the bottom. Tab moves between the two
/// focusable panels (the toolkit's own navigation — no focus manager here); the detail panel just
/// mirrors the steps selection. <see cref="Sync"/> is the single update point, driven by the
/// repaint timer on the UI thread from thread-safe snapshots.
/// </summary>
internal sealed class TuiScreen
{
    // The steps panel is a fixed column on the left; the detail panel starts where it ends.
    internal const int StepsWidth = 34;
    internal const int FluxHeight = 6;
    // rows reserved at the bottom: the flux band + the status line + the hint bar.
    internal const int BottomChrome = FluxHeight + 2;

    private readonly TuiChrome _chrome;
    private readonly Window _window;
    private readonly StepsPanel _steps = new();
    private readonly DetailPanel _detail = new();
    private readonly FluxPanel _flux = new();
    private readonly Label _status;
    private readonly Label _hints;

    private bool _expanded;

    public TuiScreen(TuiChrome chrome)
    {
        _chrome = chrome;
        _window = new Window { Title = chrome.Title };

        _status = new Label { X = 0, Y = Pos.AnchorEnd(2), Width = Dim.Fill(), Text = chrome.Status };
        _hints = new Label { X = 0, Y = Pos.AnchorEnd(1), Width = Dim.Fill(), Text = HintsFor(null) };

        _window.Add(_steps.Frame, _detail.Frame, _flux.Frame, _status, _hints);

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

        _steps.FocusTarget.SetFocus();
    }

    /// <summary>
    /// Pushes the latest state into the panels. The snapshots are already detached copies; this
    /// only touches views, on the UI thread. <paramref name="fluxLines"/> is null when the
    /// transcript has not moved since the last tick, so the flux list is left alone.
    /// </summary>
    public void Sync(IReadOnlyList<TrajectoryStep> steps, IReadOnlyList<string>? fluxLines, string clock, string meter)
    {
        _window.Title = meter.Length > 0 ? $"{_chrome.Title}    {clock} · {meter}" : $"{_chrome.Title}    {clock}";
        _status.Text = _chrome.Status;
        _steps.Update(steps);            // rebuilds only when the step count moved
        if (fluxLines is not null) _flux.Update(fluxLines);
    }

    private void RenderDetail() => _detail.Show(_steps.Selected, _expanded);

    /// <summary>
    /// Shortcuts handled above the panels: plain Tab moves between the two focusable ones (a
    /// FrameView is a tab *group*, so its own Tab stays inside — this flattens them into one ring),
    /// and while the steps panel has focus, <c>e</c>/<c>b</c> jump between errors and Right/Enter/Left
    /// expand or collapse the detail. Up/Down are left for the list itself. Shift+Tab (the E5 mode
    /// cycle) has a different KeyCode and is untouched.
    /// </summary>
    private void OnKey(Key key, View? focused)
    {
        if (key.KeyCode == KeyCode.Tab)
        {
            key.Handled = true;
            (Owns(_steps.Frame, focused) ? _flux.FocusTarget : _steps.FocusTarget).SetFocus();
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

    // ── test inspection ────────────────────────────────────────────────────────
    internal string HintsText => _hints.Text;
    internal string DetailText => _detail.BodyText;
    internal bool Expanded => _expanded;
    internal int? SelectedStepIteration => _steps.Selected?.Iteration;
    internal void FocusFlux() => _flux.FocusTarget.SetFocus();
    internal void FocusSteps() => _steps.FocusTarget.SetFocus();

    private string HintsFor(View? focused)
    {
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
