using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace DtPipe.Cli.Agent;

/// <summary>
/// Full-screen keyboard review of a finished session's trajectory: scroll the steps, expand one
/// for its reasoning / tool args / result / chain of thought, jump between the errors. A distinct
/// modal surface — the live loop stays in scrollback, this one may take the screen — with a pure
/// state machine (<see cref="Apply"/>) so a scripted key sequence is asserted without a console.
/// </summary>
internal sealed class SessionReview
{
    private const int ListWindow = 16;

    private readonly IReadOnlyList<TrajectoryStep> _steps;
    private int _selected;
    private bool _expanded;

    public SessionReview(IReadOnlyList<TrajectoryStep> steps)
    {
        _steps = steps ?? Array.Empty<TrajectoryStep>();
    }

    public int Selected => _selected;
    public bool Expanded => _expanded;
    public TrajectoryStep? Current => _steps.Count > 0 ? _steps[_selected] : null;

    /// <summary>Applies one keypress. Returns <c>false</c> when the user asked to leave.</summary>
    public bool Apply(ConsoleKeyInfo key)
    {
        switch (key.Key)
        {
            case ConsoleKey.Q:
            case ConsoleKey.Escape:
                return false;

            case ConsoleKey.UpArrow:
            case ConsoleKey.K:
                Move(-1); break;
            case ConsoleKey.DownArrow:
            case ConsoleKey.J:
                Move(+1); break;
            case ConsoleKey.PageUp:
                Move(-ListWindow); break;
            case ConsoleKey.PageDown:
                Move(+ListWindow); break;
            case ConsoleKey.Home:
                _selected = 0; break;
            case ConsoleKey.End:
                _selected = LastIndex; break;

            case ConsoleKey.RightArrow:
            case ConsoleKey.Enter:
                _expanded = true; break;
            case ConsoleKey.LeftArrow:
                _expanded = false; break;

            case ConsoleKey.E:
                JumpError(+1); break;
            case ConsoleKey.B:
                JumpError(-1); break;
        }

        return true;
    }

    private int LastIndex => Math.Max(0, _steps.Count - 1);

    private void Move(int delta)
    {
        if (_steps.Count == 0) return;
        _selected = Math.Clamp(_selected + delta, 0, LastIndex);
    }

    private void JumpError(int direction) => _selected = StepNavigation.NextError(_steps, _selected, direction);

    public IRenderable RenderFrame()
    {
        if (_steps.Count == 0)
            return new Panel(new Markup("[yellow]No trajectory steps logged in this session.[/]"));

        var grid = new Grid();
        grid.AddColumn(new GridColumn().Width(38).NoWrap());
        grid.AddColumn(new GridColumn());
        grid.AddRow(BuildList(), BuildDetail());

        return new Panel(grid)
        {
            Header = new PanelHeader(
                $"[bold cyan]Session review[/]  [grey]{_selected + 1}/{_steps.Count}[/]  "
                + "[grey]↑↓ move · →/← expand · e/b errors · q quit[/]"),
            Border = BoxBorder.Rounded,
            Expand = true,
        };
    }

    private IRenderable BuildList()
    {
        int first = Math.Clamp(_selected - ListWindow / 2, 0, Math.Max(0, _steps.Count - ListWindow));
        int last = Math.Min(_steps.Count, first + ListWindow);

        var rows = new List<IRenderable>();
        for (int i = first; i < last; i++)
        {
            var s = _steps[i];
            string icon = s.IsError ? "⚠" : s.ToolName != null ? "🛠" : "💭";
            string label = $"{s.Iteration,3} {icon} {s.ToolName ?? "reasoning"}";
            if (label.Length > 33) label = label[..32] + "…";

            rows.Add(i == _selected
                ? new Markup($"[black on grey]▸ {Markup.Escape(label)}[/]")
                : new Markup($"  {Markup.Escape(label)}"));
        }

        if (last < _steps.Count) rows.Add(new Markup("[grey]  ↓ more[/]"));
        return new Rows(rows);
    }

    private IRenderable BuildDetail()
    {
        var s = _steps[_selected];
        var parts = new List<IRenderable>();

        // The content decisions live in StepDetailContent (shared with the full-screen panel); the
        // Spectre styling of each section is this surface's own and is unchanged.
        foreach (var sec in StepDetailContent.Of(s, _expanded))
        {
            parts.Add(sec.Kind switch
            {
                DetailSectionKind.Usage =>
                    new Markup($"[grey]{Markup.Escape(sec.Body)}[/]"),
                DetailSectionKind.Reasoning =>
                    new Panel(new Markup(Markup.Escape(sec.Body)))
                    {
                        Header = new PanelHeader("[yellow]reasoning / intent[/]"),
                        Border = BoxBorder.Rounded,
                    },
                DetailSectionKind.ChainOfThought =>
                    new Panel(new Markup($"[dim]{Markup.Escape(sec.Body)}[/]"))
                    {
                        Header = new PanelHeader("[grey]chain of thought[/]"),
                        Border = BoxBorder.Rounded,
                        BorderStyle = new Style(Color.Grey35),
                    },
                DetailSectionKind.ToolName =>
                    new Markup($"[bold magenta]tool[/] {Markup.Escape(sec.Body)}"),
                DetailSectionKind.ToolArgs =>
                    new Panel(new Markup(Markup.Escape(sec.Body)))
                    {
                        Header = new PanelHeader("[grey]arguments[/]"),
                        Border = BoxBorder.Square,
                    },
                DetailSectionKind.ToolOutput =>
                    new Panel(new Markup(Markup.Escape(sec.Body)))
                    {
                        Header = new PanelHeader($"[bold {(sec.IsError ? "red" : "green")}]tool output[/]"),
                        Border = BoxBorder.Rounded,
                    },
                _ => new Markup(Markup.Escape(sec.Body)),
            });
        }

        if (!_expanded)
            parts.Add(new Markup("[grey]→ / Enter to expand this step[/]"));

        return parts.Count > 0 ? new Rows(parts) : new Markup("[grey](nothing recorded for this step)[/]");
    }
}
