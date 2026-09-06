using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Drawing;
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
/// The body scrolls and takes the focus, because the running step's text grows while it is being
/// read: <see cref="FollowGate"/> chases the newest line until someone starts reading, then holds
/// their place for as long as they keep moving through it.
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
    private readonly ListView _body;
    private readonly View _plan;
    private FollowGate _gate;
    private bool _syncing;
    private string _renderedTitle = string.Empty;
    private string _rendered = string.Empty;
    private int _foldedAt = -1;
    private IReadOnlyList<string> _lines = Array.Empty<string>();

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
            CanFocus = true,
        };
        _body = new ListView { X = 0, Y = 0, Width = Dim.Fill(), Height = Dim.Fill(), CanFocus = true };
        _body.SetSource(new ObservableCollection<string>());

        _body.HasFocusChanged += (_, _) =>
        {
            if (_body.HasFocus) _gate.Focused(Now); else _gate.Blurred();
        };
        // Viewport movement is the one signal a wheel, a drag and an arrow key all produce.
        _body.ViewportChanged += (_, _) => { if (!_syncing) _gate.Interacted(Now); };
        _body.ValueChanged += (_, _) => { if (!_syncing) _gate.Interacted(Now); };

        _frame.Add(_body);
    }

    public View Frame => _frame;
    public View FocusTarget => _body;
    internal string BodyText => string.Join('\n', _lines);

    private static long Now => Environment.TickCount64;

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
        Write(step.Text.Length > 0 ? step.Text : "(the model has not said anything yet)");
    }

    /// <summary>Renders <paramref name="step"/> at the given expand state, or a placeholder when null.</summary>
    public void Show(TrajectoryStep? step, bool expanded)
    {
        if (step is null)
        {
            Write("(no step selected yet)");
            Retitle("Detail");
            return;
        }

        // The usage belongs to the step as a whole, so it goes where the step is named. It is
        // still a section of the shared content — the scrollback review has no title to lift it
        // into — and this panel simply reads it from the title instead of the body.
        var usage = StepDetailContent.UsageBrief(step);
        Retitle($"Detail — step {step.Iteration}"
            + (expanded ? " (expanded)" : string.Empty)
            + (usage is null ? string.Empty : $" · {usage}"));

        var sb = new StringBuilder();
        foreach (var section in StepDetailContent.Of(step, expanded))
        {
            if (section.Kind == DetailSectionKind.Usage) continue;   // it is in the title

            if (sb.Length > 0) sb.Append('\n');
            sb.Append(section.Kind == DetailSectionKind.ToolName
                ? $"tool: {section.Body}"
                : section.Title.Length > 0
                    ? $"{section.Title}\n{Indent(section.Body)}"
                    : section.Body);
        }

        if (sb.Length == 0) sb.Append("(nothing recorded for this step)");
        if (!expanded) sb.Append("\n\n→ / Enter to expand");

        Write(sb.ToString());
    }

    /// <summary>
    /// Replaces the body, then either chases its newest line or puts the reader back where they
    /// were. The list is rebuilt from a fresh source, which resets both the selection and the scroll
    /// offset — without the restore, a held body would snap to the top on every arriving token.
    /// </summary>
    private void Write(string text)
    {
        // Refold when the column changes too: expanding the detail hands it the plan's width, and a
        // resize moves it again — the same text then needs different line breaks.
        int width = _body.Viewport.Width;
        if (text == _rendered && width == _foldedAt) return;
        _rendered = text;
        _foldedAt = width;
        _lines = TextWrap.Fold(text, width);

        bool follow = _gate.ShouldFollow(Now);
        int? held = _body.SelectedItem;
        int offset = _body.Viewport.Y;

        _syncing = true;
        try
        {
            _body.SetSource(new ObservableCollection<string>(_lines));
            if (_lines.Count == 0) return;

            if (follow)
            {
                _body.SelectedItem = _lines.Count - 1;
                _body.EnsureSelectedItemVisible();
                return;
            }

            if (held is { } index) _body.SelectedItem = Math.Min(index, _lines.Count - 1);
            var viewport = _body.Viewport;
            _body.Viewport = new Rectangle(viewport.X, Math.Min(offset, Math.Max(0, _lines.Count - 1)), viewport.Width, viewport.Height);
        }
        finally
        {
            _syncing = false;
        }
    }

    private void Retitle(string title)
    {
        if (_renderedTitle == title) return;
        _frame.Title = _renderedTitle = title;
    }

    private static string Indent(string body) =>
        string.Join('\n', body.Split('\n').Select(l => "  " + l));
}
