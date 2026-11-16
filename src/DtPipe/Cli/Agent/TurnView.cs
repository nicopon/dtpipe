using System;
using System.Threading;
using System.Threading.Tasks;

namespace DtPipe.Cli.Agent;

/// <summary>
/// Where one turn tells its host what is happening. The planning loop calls this and never touches
/// a console, so a turn runs, and is tested, without a terminal. What a turn did stays on
/// <see cref="AgentTrajectory"/>; a view carries only what is in flight and what the trajectory
/// does not fold.
/// </summary>
internal interface ITurnView
{
    /// <summary>
    /// Whether the view can follow a model call token by token. A view that cannot gets the
    /// blocking call, even from a client that streams.
    /// </summary>
    bool CanStream { get; }

    /// <summary>Runs one streaming model call; the view follows it through the observer it hands out.</summary>
    Task<LlmResponse> StreamingStepAsync(int step, int maxSteps,
        Func<ILlmStreamObserver, Task<LlmResponse>> call);

    /// <summary>Runs one blocking model call (no token stream).</summary>
    Task<LlmResponse> BlockingStepAsync(int step, Func<CancellationToken, Task<LlmResponse>> call, CancellationToken ct);

    /// <summary>A tool call finished, with the answer the model received.</summary>
    void ToolResult(string toolName, string result, bool isError);

    /// <summary>A fresh plan YAML was captured from a <c>yamlContent</c> tool-call argument.</summary>
    void PlanUpdated(string yaml);
}
