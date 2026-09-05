using System.Collections.Generic;
using System.Collections.ObjectModel;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

namespace DtPipe.Cli.Agent.Tui.Panels;

/// <summary>
/// The running transcript — step digests, tool results, the streaming tail — in a band along the
/// bottom. Focusable so Up/Down scroll it; otherwise it tracks the newest line.
/// </summary>
internal sealed class FluxPanel
{
    private readonly FrameView _frame;
    private readonly ListView _list;

    public FluxPanel()
    {
        _frame = new FrameView
        {
            Title = "Flux",
            X = 0,
            Y = Pos.AnchorEnd(TuiScreen.BottomChrome),
            Width = Dim.Fill(),
            Height = Dim.Absolute(TuiScreen.FluxHeight),
            CanFocus = true,
        };
        _list = new ListView
        {
            X = 0,
            Y = 0,
            Width = Dim.Fill(),
            Height = Dim.Fill(),
            CanFocus = true,
        };
        _list.SetSource(new ObservableCollection<string>());
        _frame.Add(_list);
    }

    public View Frame => _frame;
    public View FocusTarget => _list;

    /// <summary>Replaces the lines and, unless the user has scrolled up, pins the view to the end.</summary>
    public void Update(IReadOnlyList<string> lines)
    {
        bool follow = !_list.HasFocus;
        _list.SetSource(new ObservableCollection<string>(lines));
        if (follow && lines.Count > 0)
        {
            _list.SelectedItem = lines.Count - 1;
            _list.EnsureSelectedItemVisible();
        }
    }
}
