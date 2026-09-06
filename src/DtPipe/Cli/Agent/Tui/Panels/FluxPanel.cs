using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Drawing;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

namespace DtPipe.Cli.Agent.Tui.Panels;

/// <summary>
/// The running transcript — step digests, tool results, the streaming tail — in a band along the
/// bottom. It tracks the newest line until a reader takes the focus, and then holds still for as
/// long as they keep moving through it (<see cref="FollowGate"/>).
///
/// <para>
/// Holding still is the reason <see cref="Update"/> saves and restores the position: the list is
/// rebuilt from a fresh source on every transcript change, and that resets both the selection and
/// the scroll offset. Without the restore, a held band would still jump — not to the tail, but to
/// the top, on every arriving line.
/// </para>
/// </summary>
internal sealed class FluxPanel
{
    private readonly FrameView _frame;
    private readonly ListView _list;
    private FollowGate _gate;
    private bool _syncing;

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

        _list.HasFocusChanged += (_, _) =>
        {
            if (_list.HasFocus) _gate.Focused(Now); else _gate.Blurred();
        };
        // Viewport movement is the one signal a wheel, a drag and an arrow key all produce.
        _list.ViewportChanged += (_, _) => Renew();
        _list.ValueChanged += (_, _) => Renew();

        _frame.Add(_list);
    }

    public View Frame => _frame;
    public View FocusTarget => _list;
    internal int? SelectedIndex => _list.SelectedItem;

    private static long Now => Environment.TickCount64;

    /// <summary>
    /// Renews the hold for a reader-driven move. The panel's own re-pinning moves the viewport and
    /// the selection too, and counting that as reading would hold the band forever the moment it
    /// took the focus once.
    /// </summary>
    private void Renew()
    {
        if (!_syncing) _gate.Interacted(Now);
    }

    /// <summary>Replaces the lines, then either pins the view to the end or puts the reader back where they were.</summary>
    public void Update(IReadOnlyList<string> lines)
    {
        bool follow = _gate.ShouldFollow(Now);
        int? held = _list.SelectedItem;
        int offset = _list.Viewport.Y;

        _syncing = true;
        try
        {
            _list.SetSource(new ObservableCollection<string>(lines));
            if (lines.Count == 0) return;

            if (follow)
            {
                _list.SelectedItem = lines.Count - 1;
                _list.EnsureSelectedItemVisible();
                return;
            }

            // The transcript is capped to its last lines, so a long session slides the whole list
            // out from under a held position; clamping is what keeps the restore in range.
            if (held is { } index) _list.SelectedItem = Math.Min(index, lines.Count - 1);
            var viewport = _list.Viewport;
            _list.Viewport = new Rectangle(viewport.X, Math.Min(offset, Math.Max(0, lines.Count - 1)), viewport.Width, viewport.Height);
        }
        finally
        {
            _syncing = false;
        }
    }
}
