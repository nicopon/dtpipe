using System.Collections.Generic;
using System.Linq;
using System.Text;
using DtPipe.Cli.Agent;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

namespace DtPipe.Cli.Agent.Tui.Panels;

/// <summary>
/// The detail of the step highlighted in <see cref="StepsPanel"/>, the middle column. Read-only: it
/// mirrors the selection and the expand state, and shows the same <see cref="StepDetailContent"/>
/// sections the scrollback review does, flattened to plain text.
///
/// <para>
/// The step in flight has its own entry point rather than a fabricated <see cref="TrajectoryStep"/>:
/// it has not happened yet, and the shared <see cref="StepDetailContent"/> describes steps that
/// have. What it shows there is the model's streamed text, in full — there is nothing to collapse
/// while it is still arriving, so the expand state applies to recorded steps only.
/// </para>
///
/// <para>
/// It runs up to the plan panel beside it, and takes that space too once expanded — the expanded
/// detail is the one thing on this surface that genuinely wants the width, and the plan is the one
/// panel that can stand down for it.
/// </para>
/// </summary>
internal sealed class DetailPanel
{
    private readonly FrameView _frame;
    private readonly Label _body;

    private readonly View _plan;
    private string _renderedTitle = string.Empty;

    /// <param name="plan">The panel to the right; the detail runs up to it while collapsed.</param>
    public DetailPanel(View plan)
    {
        _plan = plan;
        _frame = new FrameView
        {
            Title = "Detail",
            X = Pos.Absolute(TuiScreen.StepsWidth),
            Y = 0,
            Width = Dim.Fill(plan),
            Height = Dim.Fill(TuiScreen.BottomChrome),
            CanFocus = false,
        };
        _body = new Label { X = 0, Y = 0, Width = Dim.Fill(), Height = Dim.Fill(), Text = string.Empty };
        _frame.Add(_body);
    }

    public View Frame => _frame;
    internal string BodyText => _body.Text;

    /// <summary>Takes the plan panel's column as well, or gives it back.</summary>
    public void Widen(bool wide) => _frame.Width = wide ? Dim.Fill() : Dim.Fill(_plan);

    /// <summary>
    /// Renders the step the model is producing right now: its text as it arrives, and a title that
    /// says it is still running. Called on every repaint while the turn holds it, so both writes are
    /// guarded — the text grows a token at a time and the title does not move at all.
    /// </summary>
    public void ShowLive(LiveStep step)
    {
        Retitle($"Detail — step {step.Iteration} (running)");
        var body = step.Text.Length > 0 ? step.Text : "(the model has not said anything yet)";
        if (_body.Text != body) _body.Text = body;
    }

    /// <summary>Renders <paramref name="step"/> at the given expand state, or a placeholder when null.</summary>
    public void Show(TrajectoryStep? step, bool expanded)
    {
        if (step is null)
        {
            if (_body.Text != "(no step selected yet)") _body.Text = "(no step selected yet)";
            Retitle("Detail");
            return;
        }

        Retitle($"Detail — step {step.Iteration}" + (expanded ? " (expanded)" : string.Empty));

        var sb = new StringBuilder();
        foreach (var section in StepDetailContent.Of(step, expanded))
        {
            if (sb.Length > 0) sb.Append('\n');
            sb.Append(section.Kind == DetailSectionKind.ToolName
                ? $"tool: {section.Body}"
                : section.Title.Length > 0
                    ? $"{section.Title}\n{Indent(section.Body)}"
                    : section.Body);
        }

        if (sb.Length == 0) sb.Append("(nothing recorded for this step)");
        if (!expanded) sb.Append("\n\n→ / Enter to expand");

        var text = sb.ToString();
        if (_body.Text != text) _body.Text = text;
    }

    private void Retitle(string title)
    {
        if (_renderedTitle == title) return;
        _frame.Title = _renderedTitle = title;
    }

    private static string Indent(string body) =>
        string.Join('\n', body.Split('\n').Select(l => "  " + l));
}
