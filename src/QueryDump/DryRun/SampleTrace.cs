namespace QueryDump.DryRun;

using QueryDump.Core.Abstractions;
using QueryDump.Cli.Abstractions;
using QueryDump.Core.Models;

/// <summary>
/// Represents the state of a row at a specific pipeline stage.
/// </summary>
public record StageTrace(
    IReadOnlyList<ColumnInfo> Schema,
    object?[] Values
);

/// <summary>
/// Captures the trace of a single sample row through all pipeline stages.
/// </summary>
public record SampleTrace(
    List<StageTrace> Stages
);
