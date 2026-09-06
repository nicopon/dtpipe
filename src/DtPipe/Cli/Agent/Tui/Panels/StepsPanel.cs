using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using DtPipe.Cli.Agent;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

namespace DtPipe.Cli.Agent.Tui.Panels;

/// <summary>
/// The list of trajectory steps, on the left. Up/Down and Home/End come from the <see cref="ListView"/>;
/// <c>e</c>/<c>b</c> (via <see cref="JumpToError"/>) jump to the next / previous error using
/// <see cref="StepNavigation"/> — the same logic the scrollback review uses.
/// </summary>
internal sealed class StepsPanel
{
    private readonly FrameView _frame;
    private readonly ListView _list;
    private IReadOnlyList<TrajectoryStep> _steps = Array.Empty<TrajectoryStep>();
    private int _renderedCount = -1;

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
        _list.SetSource(new ObservableCollection<string>());
        _list.ValueChanged += (_, _) => SelectionChanged?.Invoke();
        _frame.Add(_list);
    }

    public View Frame => _frame;
    public View FocusTarget => _list;

    /// <summary>The highlighted step, or null when the trajectory is still empty.</summary>
    public TrajectoryStep? Selected =>
        _steps.Count > 0 && _list.SelectedItem is { } i && i >= 0 && i < _steps.Count ? _steps[i] : null;

    /// <summary>Moves the selection to the next error in <paramref name="direction"/> (<c>+1</c> / <c>-1</c>).</summary>
    public void JumpToError(int direction)
    {
        if (_steps.Count == 0) return;
        _list.SelectedItem = StepNavigation.NextError(_steps, _list.SelectedItem ?? 0, direction);
        _list.EnsureSelectedItemVisible();
    }

    /// <summary>
    /// Replaces the rows when the snapshot grew (or shrank). A selection sitting on the last row is
    /// kept there so the panel tracks the running turn; otherwise the index is preserved. A no-op
    /// when the count has not moved — the rows for existing steps never change.
    /// </summary>
    public void Update(IReadOnlyList<TrajectoryStep> steps)
    {
        _steps = steps;
        if (steps.Count == _renderedCount) return;

        int prevIndex = _list.SelectedItem ?? 0;
        bool wasAtEnd = _renderedCount <= 0 || prevIndex >= _renderedCount - 1;
        _renderedCount = steps.Count;

        _list.SetSource(new ObservableCollection<string>(steps.Select(RowLabel)));

        if (steps.Count == 0) { SelectionChanged?.Invoke(); return; }
        _list.SelectedItem = wasAtEnd ? steps.Count - 1 : Math.Min(prevIndex, steps.Count - 1);
        _list.EnsureSelectedItemVisible();
        SelectionChanged?.Invoke();
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
