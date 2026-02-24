namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Represents the complete Directed Acyclic Graph (DAG) for a multi-branch or XStreamer job.
/// </summary>
public record JobDagDefinition
{
    /// <summary>
    /// The ordered list of branches that make up the DAG.
    /// Branches are typically executed in parallel but may have logical data dependencies
    /// (e.g., an XStreamer branch consuming data from upstream branches).
    /// </summary>
    public IReadOnlyList<BranchDefinition> Branches { get; init; } = Array.Empty<BranchDefinition>();

    /// <summary>
    /// Gets a value indicating whether this job requires DAG orchestration
    /// (i.e., it contains more than one branch or an explicit XStreamer).
    /// </summary>
    public bool IsDag => Branches.Count > 1 || Branches.Any(b => b.IsXStreamer);
}
