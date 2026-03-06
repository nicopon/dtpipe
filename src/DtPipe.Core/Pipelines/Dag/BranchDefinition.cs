namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Represents a single, linear segment of a larger Directed Acyclic Graph (DAG) pipeline.
/// A branch has its own discrete set of CLI arguments that define its Input,
/// Transformers, and Output.
/// </summary>
public record BranchDefinition
{
    /// <summary>
    /// The unique identifier or alias for this branch (e.g., "stream0", "my_source").
    /// </summary>
    public string Alias { get; init; } = string.Empty;

    /// <summary>
    /// The raw CLI arguments that define this specific branch's execution plan.
    /// This array is a slice of the original application arguments.
    /// </summary>
    public string[] Arguments { get; init; } = Array.Empty<string>();

    /// <summary>
    /// Indicates whether this branch represents an XStreamer node.
    /// XStreamer branches typically have multiple inputs (reads from other branches)
    /// and a single output.
    /// </summary>
    public bool IsXStreamer { get; init; }

    /// <summary>
    /// The input source for this branch (e.g. "csv:file.csv" or "pg:query").
    /// For XStreamer branches, this might be null or represent the provider.
    /// </summary>
    public string? Input { get; init; }

    /// <summary>
    /// The output destination for this branch (e.g. "parquet:out.pq").
    /// </summary>
    public string? Output { get; init; }

    /// <summary>
    /// For XStreamer branches, the alias of the primary source branch.
    /// </summary>
    public string? MainAlias { get; init; }

    /// <summary>
    /// For XStreamer branches, the aliases of secondary source branches.
    /// </summary>
    public IReadOnlyList<string> RefAliases { get; init; } = Array.Empty<string>();
}
