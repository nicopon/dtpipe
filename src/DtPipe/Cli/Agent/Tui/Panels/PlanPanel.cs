using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Agent;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

namespace DtPipe.Cli.Agent.Tui.Panels;

/// <summary>
/// The plan / DAG "work in progress", the right-hand column beside the detail. Read-only: one line
/// per branch and a status badge, both from <see cref="PlanPanelContent"/>. Rebuilds only when the
/// rendered lines actually change — the repaint timer calls <see cref="Update"/> every tick.
///
/// <para>
/// Anchored to the right edge at a share of the width rather than a fixed number of columns, so a
/// branch line keeps a usable width on a narrow terminal instead of the detail taking all of it.
/// </para>
/// </summary>
internal sealed class PlanPanel
{
    private readonly FrameView _frame;
    private readonly Label _body;
    private string _rendered = string.Empty;

    public PlanPanel()
    {
        _frame = new FrameView
        {
            Title = "Plan",
            X = Pos.AnchorEnd(),
            Y = 0,
            Width = Dim.Percent(TuiScreen.PlanShare),
            Height = Dim.Fill(TuiScreen.BottomChrome),
            CanFocus = false,
        };
        _body = new Label { X = 0, Y = 0, Width = Dim.Fill(), Height = Dim.Fill(), Text = string.Empty };
        _frame.Add(_body);
    }

    public View Frame => _frame;
    internal string BodyText => _body.Text;

    /// <summary>Refreshes the panel from <paramref name="plan"/>; a no-op when nothing changed.</summary>
    public void Update(PlanView plan)
    {
        var lines = PlanPanelContent.Lines(plan);
        var text = string.Join('\n', lines);
        if (text == _rendered) return;

        _rendered = text;
        _body.Text = text;
    }
}
