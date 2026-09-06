using System;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Pipeline;

namespace DtPipe.Cli.Agent.Tui;

/// <summary>
/// The turn view behind the full-screen surface: every step, tool result and answer becomes a
/// <see cref="TranscriptEntry"/> in a <see cref="TranscriptLog"/>, and the streaming step is
/// exposed as a live tail. It touches no toolkit type and marshals nothing — the surface polls
/// <see cref="TranscriptLog.Version"/> on a timer. That inversion is the point: a push per token
/// would repaint at the model's token rate.
///
/// <para>
/// It also folds the plan's progress: <see cref="PlanUpdated"/> and every <see cref="ToolResult"/>
/// feed a <see cref="PlanProgress"/>, and a fresh plan YAML is turned into a
/// <see cref="DagTopology"/>. Both are read back through <see cref="PlanSnapshot"/> under a lock —
/// the turn thread writes, the repaint timer reads.
/// </para>
/// </summary>
internal sealed class TuiTurnView : ITurnView
{
    private const int LiveTailLines = 12;

    private readonly TranscriptLog _log;
    private readonly DagTopologyService? _topology;

    // Written by the turn thread, read by the repaint timer on the UI thread.
    private volatile StreamingStepView? _streaming;

    // The plan's progress and its topology — written from the turn thread (PlanUpdated / ToolResult),
    // read from the UI thread (PlanSnapshot). PlanProgress is not itself thread-safe, hence the lock.
    private readonly object _planGate = new();
    private readonly PlanProgress _plan = new();
    private DagTopology? _planTopology;

    public TuiTurnView(TranscriptLog log, DagTopologyService? topology = null)
    {
        _log = log;
        _topology = topology;
    }

    /// <summary>
    /// The plain text of the step currently streaming, or null between steps. Read by the
    /// surface's repaint timer, never pushed.
    /// </summary>
    public string? LiveTailPlain() => _streaming?.TailPlain(LiveTailLines);

    /// <summary>
    /// The step in flight, or null between steps. It is deliberately not a
    /// <see cref="TrajectoryStep"/> and never joins <see cref="AgentTrajectory"/>: that list is the
    /// session's record, read by the scrollback review and the final summary, and a step that has
    /// not happened yet would be a lie told to both. The surface composes this one onto the end of
    /// the record for as long as it is running.
    /// </summary>
    public LiveStep? LiveStepSnapshot()
    {
        var streaming = _streaming;
        return streaming is null
            ? null
            : new LiveStep(streaming.Step, streaming.ToolName, streaming.TailPlain(LiveTailLines) ?? string.Empty);
    }

    /// <summary>A thread-safe read of the plan's state, message and topology for the plan panel.</summary>
    public PlanView PlanSnapshot()
    {
        lock (_planGate)
            return new PlanView(_plan.State, _plan.Message, _planTopology);
    }

    /// <summary>
    /// A fresh plan YAML was captured from a <c>yamlContent</c> tool argument. Feeds
    /// <see cref="PlanProgress"/> and, when the YAML actually changed, rebuilds the topology.
    /// </summary>
    public void PlanUpdated(string yaml)
    {
        lock (_planGate)
        {
            var before = _plan.Yaml;
            _plan.OnPlanUpdated(yaml);
            if (!string.Equals(before, _plan.Yaml, StringComparison.Ordinal))
                _planTopology = _topology?.TryDescribe(_plan.Yaml);
        }
    }

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
    {
        _log.Append(StepDigest.ToolResultEntry(toolName, result, isError));
        lock (_planGate) _plan.OnToolResult(toolName, isError, result);
    }

    public void AgentResponse(string content)
    {
        if (string.IsNullOrWhiteSpace(content)) return;
        _log.Append(StepDigest.AgentResponseEntry(content));
    }
}

/// <summary>
/// The step the model is producing right now, as a detached read. Its text is the streamed tail, so
/// it grows between one repaint and the next.
/// </summary>
/// <param name="Iteration">Which step of the turn it is.</param>
/// <param name="ToolName">The tool it has announced, once it has announced one.</param>
/// <param name="Text">What the model has said so far.</param>
internal readonly record struct LiveStep(int Iteration, string? ToolName, string Text);
