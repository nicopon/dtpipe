namespace QueryDump.DryRun;

using QueryDump.Core;

/// <summary>
/// Captures the trace of a single sample row through all pipeline stages.
/// </summary>
public record SampleTrace(
    /// <summary>Schema at each pipeline stage (input + each transformer output)</summary>
    List<IReadOnlyList<ColumnInfo>> Schemas,
    /// <summary>Values at each pipeline stage (input + each transformer output)</summary>
    List<object?[]> Values
);
