using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using DtPipe.Cli.Agent;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

namespace DtPipe.Cli.Agent.Tui.Panels;

/// <summary>
/// The list of trajectory steps, on the left, with the step in flight appended while a turn runs.
/// Up/Down and Home/End come from the <see cref="ListView"/>; <c>e</c>/<c>b</c> (via
/// <see cref="JumpToError"/>) jump to the next / previous error using <see cref="StepNavigation"/> —
/// the same logic the scrollback review uses.
///
/// <para>
/// The rows are mutated in place rather than replaced. The running row carries a turning wheel, so
/// it changes on every repaint, and rebuilding the source for that would clear the selection ten
/// times a second — which is exactly what reading an earlier step while the turn continues must not
/// do.
/// </para>
/// </summary>
internal sealed class StepsPanel
{
    private readonly FrameView _frame;
    private readonly ListView _list;
    private readonly ObservableCollection<string> _rows = new();
    private IReadOnlyList<TrajectoryStep> _steps = Array.Empty<TrajectoryStep>();
    private LiveStep? _live;
    private int _committed;

    /// <summary>Raised when the highlighted step changes (keyboard, or a fresh <see cref="Update"/>).</summary>
    public event Action? SelectionChanged;

    public StepsPanel()
    {
        _frame = new FrameView
        {
            Title = "Steps",
            X = 0,
            Y = 0,
            Width = Dim.Absolute(TuiScreen.StepsWidth),
            Height = Dim.Fill(TuiScreen.BottomChrome),
            CanFocus = true,
        };
        _list = new ListView
        {
            X = 0,
            Y = 0,
            Width = Dim.Fill(),
            Height = Dim.Fill(),
            CanFocus = true,
            // Letters mean "jump to error" here, not type-ahead search.
            KeystrokeNavigator = null,
        };
        _list.SetSource(_rows);
        _list.ValueChanged += (_, _) => SelectionChanged?.Invoke();
        _frame.Add(_list);
    }

    public View Frame => _frame;
    public View FocusTarget => _list;

    /// <summary>The highlighted recorded step, or null when the trajectory is empty or the running row is highlighted.</summary>
    public TrajectoryStep? Selected =>
        _steps.Count > 0 && _list.SelectedItem is { } i && i >= 0 && i < _steps.Count ? _steps[i] : null;

    /// <summary>Whether the highlighted row is the step in flight rather than a recorded one.</summary>
    public bool LiveSelected => _live is not null && _list.SelectedItem == _committed;

    /// <summary>Puts the highlight on the last row, so the panel tracks the turn again.</summary>
    public void SelectLast()
    {
        if (_rows.Count == 0) return;
        _list.SelectedItem = _rows.Count - 1;
        _list.EnsureSelectedItemVisible();
        SelectionChanged?.Invoke();
    }

    /// <summary>Moves the selection to the next error in <paramref name="direction"/> (<c>+1</c> / <c>-1</c>).</summary>
    public void JumpToError(int direction)
    {
        if (_steps.Count == 0) return;
        _list.SelectedItem = StepNavigation.NextError(_steps, _list.SelectedItem ?? 0, direction);
        _list.EnsureSelectedItemVisible();
    }

    /// <summary>
    /// Brings the rows up to date: the recorded steps, then the running one if there is one. A
    /// selection sitting on the last row is kept there so the panel tracks the running turn;
    /// otherwise the index is preserved.
    /// </summary>
    /// <param name="spinner">The wheel's current frame, so this row turns in step with the caret.</param>
    public void Update(IReadOnlyList<TrajectoryStep> steps, LiveStep? live, string spinner)
    {
        _steps = steps;
        _live = live;

        int before = _rows.Count;
        int previous = _list.SelectedItem ?? 0;
        bool wasAtEnd = before == 0 || previous >= before - 1;

        // Recorded steps only accumulate, and a row never changes once written.
        for (int i = _committed; i < steps.Count; i++) _rows.Insert(i, RowLabel(steps[i]));
        _committed = steps.Count;

        string? running = live is { } step ? RunningLabel(step, spinner) : null;
        bool hadRunning = _rows.Count > _committed;
        if (running is null)
        {
            if (hadRunning) _rows.RemoveAt(_rows.Count - 1);
        }
        else if (hadRunning)
        {
            if (_rows[^1] != running) _rows[^1] = running;
        }
        else
        {
            _rows.Add(running);
        }

        if (_rows.Count == 0 || _rows.Count == before) return;

        _list.SelectedItem = wasAtEnd ? _rows.Count - 1 : Math.Min(previous, _rows.Count - 1);
        _list.EnsureSelectedItemVisible();
        SelectionChanged?.Invoke();
    }

    /// <summary>
    /// The running row. The wheel is its marker — the same thing the caret says at the same moment,
    /// and it needs no colour, so it holds in any terminal theme.
    /// </summary>
    private static string RunningLabel(LiveStep step, string spinner)
    {
        string label = $"{step.Iteration,3} {spinner} {step.ToolName ?? "thinking"}";
        return label.Length > 30 ? label[..29] + "…" : label;
    }

    /// <summary>
    /// One row: the iteration, what the step did, and its tool. Filled circle = it called a tool,
    /// hollow = it only reasoned, warning = it failed.
    ///
    /// <para>
    /// The glyphs stay inside the Basic Multilingual Plane on purpose. An astral character is an
    /// emoji, a terminal draws most of those two cells wide, and the budget here is counted in
    /// UTF-16 code units — so an emoji icon both swallowed the space after itself and spent two of
    /// the thirty characters this row is allowed.
    /// </para>
    /// </summary>
    internal static string RowLabel(TrajectoryStep s)
    {
        string icon = s.IsError ? "⚠" : s.ToolName != null ? "●" : "○";
        string label = $"{s.Iteration,3} {icon} {s.ToolName ?? "reasoning"}";
        return label.Length > 30 ? label[..29] + "…" : label;
    }
}
