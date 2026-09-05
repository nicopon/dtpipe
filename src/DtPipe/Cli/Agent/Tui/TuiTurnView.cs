using System;
using System.Threading;
using System.Threading.Tasks;

namespace DtPipe.Cli.Agent.Tui;

/// <summary>
/// The turn view behind the full-screen surface: every step, tool result and answer becomes a
/// <see cref="TranscriptEntry"/> in a <see cref="TranscriptLog"/>, and the streaming step is
/// exposed as a live tail. It touches no toolkit type and marshals nothing — the surface polls
/// <see cref="TranscriptLog.Version"/> on a timer. That inversion is the point: a push per token
/// would repaint at the model's token rate.
/// </summary>
internal sealed class TuiTurnView : ITurnView
{
    private const int LiveTailLines = 12;

    private readonly TranscriptLog _log;

    // Written by the turn thread, read by the repaint timer on the UI thread.
    private volatile StreamingStepView? _streaming;

    public TuiTurnView(TranscriptLog log) => _log = log;

    /// <summary>
    /// The plain text of the step currently streaming, or null between steps. Read by the
    /// surface's repaint timer, never pushed.
    /// </summary>
    public string? LiveTailPlain() => _streaming?.TailPlain(LiveTailLines);

    public async Task<LlmResponse> StreamingStepAsync(int step, int maxSteps, AgentDetailLevel detail,
        Func<ILlmStreamObserver, Task<LlmResponse>> call)
    {
        var view = new StreamingStepView(step, maxSteps);
        _streaming = view;
        LlmResponse result;
        try
        {
            // The observer accumulates into the view under its own lock; the repaint that the
            // no-op refresh would have triggered is the surface's timer instead.
            result = await call(new LiveStreamObserver(view, static () => { }));
        }
        finally
        {
            _streaming = null;
            _log.LiveTail = null;
        }

        AppendDigest(step, maxSteps, view.Elapsed, result, detail, view.ToolName);
        return result;
    }

    public async Task<LlmResponse> BlockingStepAsync(int step, Func<CancellationToken, Task<LlmResponse>> call, CancellationToken ct)
    {
        _log.LiveTail = $"· Step {step} — thinking…";
        try
        {
            return await call(ct);
        }
        finally
        {
            _log.LiveTail = null;
        }
    }

    public void Digest(int step, int maxSteps, TimeSpan elapsed, LlmResponse response, AgentDetailLevel detail)
        => AppendDigest(step, maxSteps, elapsed, response, detail, null);

    private void AppendDigest(int step, int maxSteps, TimeSpan elapsed, LlmResponse response,
        AgentDetailLevel detail, string? toolFallback)
    {
        _log.Append(StepDigest.DigestEntry(step, maxSteps, elapsed, response, toolFallback, detail));

        if (detail == AgentDetailLevel.Full && !string.IsNullOrWhiteSpace(response.Thinking))
            _log.Append(StepDigest.ThinkingEntry(response.Thinking));
    }

    public void ToolResult(string toolName, string result, bool isError)
        => _log.Append(StepDigest.ToolResultEntry(toolName, result, isError));

    public void AgentResponse(string content)
    {
        if (string.IsNullOrWhiteSpace(content)) return;
        _log.Append(StepDigest.AgentResponseEntry(content));
    }
}
