using System.Diagnostics;
using System.Linq;
using System.Text;

namespace DtPipe.Cli.Agent;

/// <summary>
/// The live view of one agent step while the model streams. Thinking and answer text accumulate
/// here for display only: the authoritative full text comes back on the <see cref="LlmResponse"/>.
/// Every mutator and <see cref="TailPlain"/> are guarded by one lock, because the stream observer
/// writes from the model call's thread while a poller reads.
/// </summary>
internal sealed class StreamingStepView
{
    private const int ThinkingDisplayCap = 6000;   // keep the tail of a long reasoning block

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

    /// <summary>
    /// The step so far as plain text, a header line on top: what it is doing, for how long, then
    /// the last <paramref name="maxLines"/> lines of the reasoning and of the answer.
    /// </summary>
    public string TailPlain(int maxLines)
    {
        lock (_gate)
        {
            var sb = new StringBuilder(HeaderPlainLocked());
            if (_thinking.Length > 0)
                sb.Append('\n').Append(Tail(_thinking.ToString(), maxLines));
            if (_content.Length > 0)
                sb.Append('\n').Append(Tail(_content.ToString(), maxLines));
            return sb.ToString();
        }
    }

    private string PhaseLocked() =>
        _tool != null ? $"calling {_tool}"
        : _sawContent ? "answering"
        : _sawThinking ? "thinking" : "waiting for first token";

    private string HeaderPlainLocked() =>
        $"· Step {_step}/{_maxSteps}  {PhaseLocked()} · {_sw.Elapsed.TotalSeconds:F0}s";

    /// <summary>Which step of the turn this is.</summary>
    public int Step => _step;

    /// <summary>The tool name seen on the stream, if any.</summary>
    public string? ToolName { get { lock (_gate) return _tool; } }

    private static string Tail(string text, int maxLines)
    {
        var parts = text.Replace("\r", "").Split('\n');
        if (parts.Length <= maxLines) return text.TrimEnd('\n');
        return string.Join('\n', parts.Skip(parts.Length - maxLines));
    }
}

/// <summary>Feeds a <see cref="StreamingStepView"/> from the client's stream.</summary>
internal sealed class LiveStreamObserver : ILlmStreamObserver
{
    private readonly StreamingStepView _view;

    public LiveStreamObserver(StreamingStepView view) => _view = view;

    public void OnThinking(string delta) => _view.AddThinking(delta);
    public void OnContent(string delta) => _view.AddContent(delta);
    public void OnToolCall(string toolName) => _view.SetTool(toolName);
}
