using System;
using System.Collections.Generic;
using System.Linq;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace DtPipe.Cli.Agent;

/// <summary>
/// The persistent Live frame for a turn: a fixed header, the tail of the transcript that fits the
/// terminal, a status line, a hint bar and the input line. <see cref="RenderFrame"/> is pure — it
/// takes the height budget as an argument — so the exact composition is asserted on a capture
/// console. The turn loop appends step digests here instead of writing them straight to scrollback.
/// </summary>
internal sealed class AgentShell
{
    // header (1) + divider (1) + status (1) + hints (1) + input (1)
    private const int Chrome = 5;
    private const int MinBody = 3;

    private readonly object _gate = new();
    private readonly List<string> _transcript = new();

    public string Title { get; set; } = "dtpipe agent";
    public string Subtitle { get; set; } = string.Empty;   // model · mode
    public string Clock { get; set; } = string.Empty;      // elapsed
    public string Meter { get; set; } = string.Empty;      // tokens · tok/s
    public string Status { get; set; } = string.Empty;     // ▸ plan · dry-run · detail:peek
    public string Hints { get; set; } = string.Empty;      // esc stop · ⇥ mode · ^O detail
    public string InputLine { get; set; } = string.Empty;  // LineEditor.RenderMarkup()

    /// <summary>The step currently streaming — shown below the transcript, not yet committed to it.</summary>
    public string? LiveTail { get; set; }

    /// <summary>A snapshot of the committed transcript, safe to read while the turn is still running.</summary>
    public IReadOnlyList<string> Transcript
    {
        get { lock (_gate) return _transcript.ToArray(); }
    }

    public void Append(string markupLine)
    {
        lock (_gate) _transcript.Add(markupLine);
    }

    public void AppendRange(IEnumerable<string> markupLines)
    {
        lock (_gate) _transcript.AddRange(markupLines);
    }

    /// <summary>The whole frame, clipped to <paramref name="height"/> total terminal rows.</summary>
    public IRenderable RenderFrame(int height)
    {
        int body = Math.Max(MinBody, height - Chrome);

        string left = $"[bold cyan]{Markup.Escape(Title)}[/]"
            + (Subtitle.Length > 0 ? $"  [grey]{Markup.Escape(Subtitle)}[/]" : string.Empty);
        string right = string.Join("  ", new[] { Clock, Meter }
            .Where(s => !string.IsNullOrEmpty(s)).Select(Markup.Escape));

        var header = new Grid();
        header.AddColumn(new GridColumn());
        header.AddColumn(new GridColumn().RightAligned());
        header.AddRow(
            new Markup(left),
            new Markup(right.Length > 0 ? $"[grey]{right}[/]" : string.Empty));

        var lines = VisibleBodyLines(body);
        IRenderable transcript = lines.Count > 0
            ? new Rows(lines.Select(l => (IRenderable)new Markup(l)))
            : new Markup(string.Empty);

        return new Rows(
            header,
            new Rule { Style = new Style(Color.Grey), Border = BoxBorder.Heavy },
            transcript,
            new Rule { Style = new Style(Color.Grey) },
            new Markup(Status.Length > 0 ? $"[grey]{Markup.Escape(Status)}[/]" : string.Empty),
            new Markup(Hints.Length > 0 ? $"[grey]{Markup.Escape(Hints)}[/]" : string.Empty),
            new Markup(InputLine.Length > 0 ? InputLine : "[grey]›[/] "));
    }

    /// <summary>The last <paramref name="body"/> rendered lines of transcript + live tail.</summary>
    internal List<string> VisibleBodyLines(int body)
    {
        List<string> all;
        lock (_gate) all = new List<string>(_transcript);

        var tail = LiveTail;   // one read: the field may be reassigned on another thread
        if (!string.IsNullOrEmpty(tail))
            all.AddRange(tail.Replace("\r", string.Empty).Split('\n'));

        return all.Count <= body ? all : all.Skip(all.Count - body).ToList();
    }
}
