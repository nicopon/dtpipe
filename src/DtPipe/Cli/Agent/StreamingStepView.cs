using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace DtPipe.Cli.Agent;

/// <summary>
/// The live, in-place view of one agent step while the model streams. Thinking and answer text
/// accumulate here for display only — the authoritative full text comes back on the
/// <see cref="LlmResponse"/>. Every mutator and <see cref="Build"/> are guarded by one lock because
/// the refresh ticker and the stream observer touch this from different threads.
/// </summary>
internal sealed class StreamingStepView
{
    private const int ThinkingDisplayCap = 6000;   // keep the tail of a long reasoning block
    private const int BodyLines = 14;              // height ceiling for the live panel

    private static readonly char[] Spin = { '⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏' };

    private readonly object _gate = new();
    private readonly StringBuilder _thinking = new();
    private readonly StringBuilder _content = new();
    private readonly Stopwatch _sw = Stopwatch.StartNew();
    private readonly int _step;
    private readonly int _maxSteps;

    private string? _tool;
    private bool _sawThinking;
    private bool _sawContent;

    public StreamingStepView(int step, int maxSteps)
    {
        _step = step;
        _maxSteps = maxSteps;
    }

    public void AddThinking(string s)
    {
        lock (_gate)
        {
            _sawThinking = true;
            _thinking.Append(s);
            if (_thinking.Length > ThinkingDisplayCap)
                _thinking.Remove(0, _thinking.Length - ThinkingDisplayCap);
        }
    }

    public void AddContent(string s)
    {
        lock (_gate)
        {
            _sawContent = true;
            _content.Append(s);
        }
    }

    public void SetTool(string tool)
    {
        lock (_gate) { _tool = tool; }
    }

    public IRenderable Build()
    {
        lock (_gate)
        {
            return new Panel(new Markup(BodyMarkupLocked(BodyLines)))
            {
                Header = new PanelHeader(HeaderMarkupLocked()),
                Border = BoxBorder.Rounded,
                BorderStyle = new Style(Color.Grey),
                Expand = true
            };
        }
    }

    /// <summary>
    /// The same live content as <see cref="Build"/> but flattened to a markup string with a header
    /// line on top — for the persistent shell, which shows it as <c>AgentShell.LiveTail</c> rather
    /// than in a bordered panel of its own.
    /// </summary>
    public string TailMarkup(int maxLines)
    {
        lock (_gate)
        {
            var body = BodyMarkupLocked(maxLines);
            return HeaderMarkupLocked() + "\n" + body;
        }
    }

    private string HeaderMarkupLocked()
    {
        string phase = _tool != null ? $"calling [magenta]{Markup.Escape(_tool)}[/]"
            : _sawContent ? "answering"
            : _sawThinking ? "thinking" : "waiting for first token";
        char spin = Spin[(int)(_sw.ElapsedMilliseconds / 100) % Spin.Length];
        return $"[blue]{spin}[/] [bold]Step {_step}/{_maxSteps}[/]  [grey]{phase} · {_sw.Elapsed.TotalSeconds:F0}s[/]";
    }

    private string BodyMarkupLocked(int maxLines)
    {
        var lines = new StringBuilder();
        if (_thinking.Length > 0)
            lines.Append("[dim]").Append(Markup.Escape(Tail(_thinking.ToString(), maxLines))).Append("[/]");
        if (_content.Length > 0)
        {
            if (lines.Length > 0) lines.Append('\n');
            lines.Append(Markup.Escape(Tail(_content.ToString(), maxLines)));
        }
        if (lines.Length == 0)
            lines.Append("[grey]…[/]");
        return lines.ToString();
    }

    /// <summary>Wall-clock since the step started — the elapsed figure for the permanent trace.</summary>
    public TimeSpan Elapsed => _sw.Elapsed;

    /// <summary>The tool name seen on the stream, if any — a fallback when the assembled response
    /// somehow lost its tool call. <see cref="StepDigest"/> prefers the response's own.</summary>
    public string? ToolName { get { lock (_gate) return _tool; } }

    public bool HasThinking { get { lock (_gate) return _thinking.Length > 0; } }

    private static string Tail(string text, int maxLines)
    {
        var parts = text.Replace("\r", "").Split('\n');
        if (parts.Length <= maxLines) return text.TrimEnd('\n');
        return string.Join('\n', parts.Skip(parts.Length - maxLines));
    }
}

/// <summary>Feeds a <see cref="StreamingStepView"/> from the client's stream and asks for a repaint.</summary>
internal sealed class LiveStreamObserver : ILlmStreamObserver
{
    private readonly StreamingStepView _view;
    private readonly Action _refresh;

    public LiveStreamObserver(StreamingStepView view, Action refresh)
    {
        _view = view;
        _refresh = refresh;
    }

    public void OnThinking(string delta) { _view.AddThinking(delta); _refresh(); }
    public void OnContent(string delta) { _view.AddContent(delta); _refresh(); }
    public void OnToolCall(string toolName) { _view.SetTool(toolName); _refresh(); }
}
