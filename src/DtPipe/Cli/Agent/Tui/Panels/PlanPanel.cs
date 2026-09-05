using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Agent;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

namespace DtPipe.Cli.Agent.Tui.Panels;

/// <summary>
/// The plan / DAG "work in progress", below the detail on the right. Read-only: one line per branch
/// and a status badge, both from <see cref="PlanPanelContent"/>. Rebuilds only when the rendered
/// lines actually change — the repaint timer calls <see cref="Update"/> every tick.
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
            X = Pos.Absolute(TuiScreen.StepsWidth),
            Y = Pos.AnchorEnd(TuiScreen.BottomChrome + TuiScreen.PlanHeight),
            Width = Dim.Fill(),
            Height = Dim.Absolute(TuiScreen.PlanHeight),
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
