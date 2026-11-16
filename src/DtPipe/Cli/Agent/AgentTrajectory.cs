using System;
using System.Collections.Generic;

namespace DtPipe.Cli.Agent;

public class TrajectoryStep
{
    public int Iteration { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.Now;
    public string Reasoning { get; set; } = string.Empty;

    /// <summary>The model's chain of thought for this step, when it exposed one. Kept out of the
    /// conversation sent back to the model — for display and inspection only.</summary>
    public string? Thinking { get; set; }

    /// <summary>Provider-reported token counts / timing for this step's generation.</summary>
    public LlmUsage? Usage { get; set; }

    public string? ToolName { get; set; }
    public string? ToolArgs { get; set; }
    public string? ToolResult { get; set; }
    public bool IsError { get; set; }
}

public class AgentTrajectory
{
    private readonly object _gate = new();

    /// <summary>
    /// Raised on the turn thread once a step is recorded. A diagnostic trace subscribes rather than
    /// being called at each site that appends one — a site added later would otherwise be
    /// silently missing from the record.
    /// </summary>
    public event Action<TrajectoryStep>? StepAdded;

    public List<TrajectoryStep> Steps { get; } = new();
    public string? LastGeneratedYaml { get; set; }

    public void AddStep(int iteration, string reasoning, string? toolName = null, string? toolArgs = null, string? toolResult = null, bool isError = false,
        string? thinking = null, LlmUsage? usage = null)
    {
        var step = new TrajectoryStep
        {
            Iteration = iteration,
            Timestamp = DateTime.Now,
            Reasoning = reasoning,
            Thinking = thinking,
            Usage = usage,
            ToolName = toolName,
            ToolArgs = toolArgs,
            ToolResult = toolResult,
            IsError = isError
        };
        // A poller reads this list while the turn thread appends to it — the lock keeps a snapshot
        // from tearing. Every other reader runs after the turn.
        lock (_gate) Steps.Add(step);
        StepAdded?.Invoke(step);
    }

    /// <summary>A copy safe to enumerate while a turn thread is still calling <see cref="AddStep"/>.</summary>
    public IReadOnlyList<TrajectoryStep> Snapshot()
    {
        lock (_gate) return Steps.ToArray();
    }
}
