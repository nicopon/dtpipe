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
    public List<TrajectoryStep> Steps { get; } = new();
    public string? LastGeneratedYaml { get; set; }

    /// <summary>
    /// Determinism report produced when the validated plan is replicated N times (<c>--repeat</c>).
    /// Null when replication is not requested.
    /// </summary>
    public DeterminismReport? Determinism { get; set; }

    public void AddStep(int iteration, string reasoning, string? toolName = null, string? toolArgs = null, string? toolResult = null, bool isError = false,
        string? thinking = null, LlmUsage? usage = null)
    {
        Steps.Add(new TrajectoryStep
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
        });
    }
}

/// <summary>
/// Result of replicating a validated plan N times to measure determinism.
/// </summary>
public class DeterminismReport
{
    /// <summary>Number of replications executed.</summary>
    public int Repetitions { get; init; }

    /// <summary>Distinct generated YAML payloads observed across replications.</summary>
    public List<string> DistinctYaml { get; init; } = new();

    /// <summary>Number of distinct YAML payloads observed (1 => fully deterministic).</summary>
    public int DistinctCount => DistinctYaml.Count;

    /// <summary>
    /// Amount of variance: the number of distinct payloads beyond the canonical one.
    /// 0 => every replication produced byte-for-byte identical YAML (fully deterministic).
    /// </summary>
    public int Variance => Math.Max(0, DistinctYaml.Count - 1);

    /// <summary>True when every replication produced byte-for-byte identical YAML.</summary>
    public bool IsDeterministic => Variance == 0;

    /// <summary>Whether at least one YAML payload was observed.</summary>
    public bool HasYaml => DistinctYaml.Count > 0;
}
