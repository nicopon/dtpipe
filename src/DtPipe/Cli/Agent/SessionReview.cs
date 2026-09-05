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

    private void JumpError(int direction)
    {
        for (int i = _selected + direction; i >= 0 && i < _steps.Count; i += direction)
        {
            if (_steps[i].IsError) { _selected = i; return; }
        }
    }

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

        if (s.Usage is { } u)
        {
            var m = new List<string>();
            if (u.PromptTokens > 0) m.Add($"prompt {u.PromptTokens} tok");
            if (u.CompletionTokens > 0) m.Add($"output {u.CompletionTokens} tok");
            if (u.TokensPerSecond is { } tps) m.Add($"{tps.ToString("F0", CultureInfo.InvariantCulture)} tok/s");
            if (m.Count > 0) parts.Add(new Markup($"[grey]{Markup.Escape(string.Join("  ·  ", m))}[/]"));
        }

        if (!string.IsNullOrWhiteSpace(s.Reasoning))
            parts.Add(new Panel(new Markup(Markup.Escape(Clip(s.Reasoning, _expanded ? 40 : 4))))
            {
                Header = new PanelHeader("[yellow]reasoning / intent[/]"),
                Border = BoxBorder.Rounded,
            });

        if (_expanded && !string.IsNullOrWhiteSpace(s.Thinking))
            parts.Add(new Panel(new Markup($"[dim]{Markup.Escape(Clip(s.Thinking!, 40))}[/]"))
            {
                Header = new PanelHeader("[grey]chain of thought[/]"),
                Border = BoxBorder.Rounded,
                BorderStyle = new Style(Color.Grey35),
            });

        if (!string.IsNullOrEmpty(s.ToolName))
        {
            parts.Add(new Markup($"[bold magenta]tool[/] {Markup.Escape(s.ToolName)}"));
            if (_expanded && !string.IsNullOrWhiteSpace(s.ToolArgs))
                parts.Add(new Panel(new Markup(Markup.Escape(Clip(s.ToolArgs!, 20))))
                {
                    Header = new PanelHeader("[grey]arguments[/]"),
                    Border = BoxBorder.Square,
                });
            if (!string.IsNullOrWhiteSpace(s.ToolResult))
            {
                string color = s.IsError ? "red" : "green";
                parts.Add(new Panel(new Markup(Markup.Escape(Clip(s.ToolResult!, _expanded ? 40 : 6))))
                {
                    Header = new PanelHeader($"[bold {color}]tool output[/]"),
                    Border = BoxBorder.Rounded,
                });
            }
        }

        if (!_expanded)
            parts.Add(new Markup("[grey]→ / Enter to expand this step[/]"));

        return parts.Count > 0 ? new Rows(parts) : new Markup("[grey](nothing recorded for this step)[/]");
    }

    private static string Clip(string text, int maxLines)
    {
        var lines = text.Replace("\r", "").Split('\n');
        if (lines.Length <= maxLines) return text.TrimEnd('\n');
        return string.Join('\n', lines.Take(maxLines)) + "\n…";
    }
}
