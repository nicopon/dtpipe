using DtPipe.Core.Models;

namespace DtPipe.DryRun;

/// <summary>
/// Represents the state of a row at a specific pipeline stage.
/// </summary>
public record StageTrace(
    IReadOnlyList<PipeColumnInfo> Schema,
    object?[]? Values
);

/// <summary>
/// Captures the trace of a single sample row through all pipeline stages.
/// </summary>
public record SampleTrace(
    List<StageTrace> Stages
);
