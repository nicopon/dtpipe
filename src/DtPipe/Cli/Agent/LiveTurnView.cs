using System;
using System.Threading;
using System.Threading.Tasks;

namespace DtPipe.Cli.Agent;

/// <summary>
/// The turn view a poller reads: the step in flight and the plan's progress. It touches no console
/// and keeps no journal (the finished steps are on <see cref="AgentTrajectory"/>), and nothing is
/// pushed: a reader asks for <see cref="LiveStepSnapshot"/> and <see cref="PlanSnapshot"/> when it
/// wants them. That inversion is the point: a push per token would follow the model's token rate.
///
/// <para>
/// It also folds the plan's progress: <see cref="PlanUpdated"/> and every <see cref="ToolResult"/>
/// feed a <see cref="PlanProgress"/>, read back through <see cref="PlanSnapshot"/> under a lock,
/// since the turn thread writes and the reader polls.
/// </para>
/// </summary>
internal sealed class LiveTurnView : ITurnView
{
    private const int LiveTailLines = 12;

    // Written by the turn thread, read by the poller.
    private volatile StreamingStepView? _streaming;
    private volatile int _blockingStep;

    // PlanProgress is not itself thread-safe, hence the lock.
    private readonly object _planGate = new();
    private readonly PlanProgress _plan = new();

    public bool CanStream => true;

    /// <summary>
    /// The step in flight, or null between steps. It is deliberately not a
    /// <see cref="TrajectoryStep"/> and never joins <see cref="AgentTrajectory"/>: that list is the
    /// session's record, and a step that has not happened yet would be a lie told to its readers.
    /// A reader composes this one onto the end of the record for as long as it is running.
    /// </summary>
    public LiveStep? LiveStepSnapshot()
    {
        var streaming = _streaming;
        if (streaming is not null)
            return new LiveStep(streaming.Step, streaming.ToolName, streaming.TailPlain(LiveTailLines));

        var blocking = _blockingStep;
        return blocking > 0
            ? new LiveStep(blocking, null, $"· Step {blocking}  thinking")
            : null;
    }

    /// <summary>A thread-safe read of the plan's state and its last message.</summary>
    public PlanView PlanSnapshot()
    {
        lock (_planGate)
            return new PlanView(_plan.State, _plan.Message);
    }

    public void PlanUpdated(string yaml)
    {
        lock (_planGate) _plan.OnPlanUpdated(yaml);
    }

    public async Task<LlmResponse> StreamingStepAsync(int step, int maxSteps,
        Func<ILlmStreamObserver, Task<LlmResponse>> call)
    {
        var view = new StreamingStepView(step, maxSteps);
        _streaming = view;
        try
        {
            return await call(new LiveStreamObserver(view));
        }
        finally
        {
            _streaming = null;
        }
    }

    public async Task<LlmResponse> BlockingStepAsync(int step, Func<CancellationToken, Task<LlmResponse>> call, CancellationToken ct)
    {
        _blockingStep = step;
        try
        {
            return await call(ct);
        }
        finally
        {
            _blockingStep = 0;
        }
    }

    public void ToolResult(string toolName, string result, bool isError)
    {
        lock (_planGate) _plan.OnToolResult(toolName, isError, result);
    }
}

/// <summary>
/// The step the model is producing right now, as a detached read. Its text is the streamed tail, so
/// it grows between one read and the next.
/// </summary>
/// <param name="Iteration">Which step of the turn it is.</param>
/// <param name="ToolName">The tool it has announced, once it has announced one.</param>
/// <param name="Text">What the model has said so far.</param>
internal readonly record struct LiveStep(int Iteration, string? ToolName, string Text);

/// <summary>The plan's progress as a poller reads it: the badge and the detail of its last failure.</summary>
internal readonly record struct PlanView(PlanState State, string? Message);
