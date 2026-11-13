using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;

namespace DtPipe.Tests.Helpers;

/// <summary>
/// A turn view that writes down what the loop told it, and has no console behind it. A test that
/// runs a turn through it runs the executor exactly as a host does, minus the host.
/// </summary>
internal sealed class RecordingTurnView : ITurnView
{
    private readonly object _gate = new();

    /// <summary>What the loop told the view, in order: <c>stream N</c>, <c>block N</c>,
    /// <c>result tool</c> or <c>result tool (error)</c>, <c>plan</c>.</summary>
    public List<string> Events { get; } = new();

    public bool CanStream { get; init; }

    public Task<LlmResponse> StreamingStepAsync(int step, int maxSteps, Func<ILlmStreamObserver, Task<LlmResponse>> call)
    {
        Record($"stream {step}");
        return call(NullLlmStreamObserver.Instance);
    }

    public Task<LlmResponse> BlockingStepAsync(int step, Func<CancellationToken, Task<LlmResponse>> call, CancellationToken ct)
    {
        Record($"block {step}");
        return call(ct);
    }

    public void ToolResult(string toolName, string result, bool isError)
        => Record(isError ? $"result {toolName} (error)" : $"result {toolName}");

    public void PlanUpdated(string yaml) => Record("plan");

    private void Record(string @event)
    {
        lock (_gate) Events.Add(@event);
    }
}

/// <summary>Runs a turn the way the unit tests of the loop need it: no console, a view that records.</summary>
internal static class AgentTurns
{
    public static Task<TurnSummaryModel> TurnAsync(
        this AgentExecutor executor,
        string prompt,
        string model = "m",
        string baseUrl = "http://x",
        AgentOptions? options = null,
        int maxIterations = 25,
        ITurnView? view = null,
        CancellationToken stop = default,
        CancellationToken ct = default)
        => executor.RunTurnAsync(prompt, model, baseUrl, options ?? new AgentOptions(), maxIterations,
            view ?? new RecordingTurnView(), stop, ct);
}
