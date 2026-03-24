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
    /// The input source for this branch (e.g. "csv:file.csv" or "pg:query").
    /// Null for stream-transformer branches (they read directly from Arrow channels).
    /// </summary>
    public string? Input { get; init; }

    /// <summary>
    /// The output destination for this branch (e.g. "parquet:out.pq").
    /// </summary>
    public string? Output { get; init; }

    /// <summary>
    /// All streaming upstream sources declared via <c>--from a,b,c</c> (comma-separated).
    /// Fan-out consumers have exactly one entry. Stream-transformer branches (merge, etc.)
    /// may have multiple entries. SQL processor branches have exactly one entry.
    /// </summary>
    public IReadOnlyList<string> StreamingAliases { get; init; } = Array.Empty<string>();

    /// <summary>
    /// Secondary source branches that are fully preloaded into memory before query execution
    /// (declared via <c>--ref a,b</c>, comma-separated). Used by SQL transformer branches.
    /// </summary>
    public IReadOnlyList<string> RefAliases { get; init; } = Array.Empty<string>();

    /// <summary>
    /// The explicit processor mode flag detected in <see cref="Arguments"/> (e.g. <c>"merge"</c>
    /// for <c>--merge</c>). Null for fan-out consumer branches and linear branches.
    /// </summary>
    public string? ProcessorName { get; init; }

    /// <summary>
    /// <c>true</c> when this branch activates a stream transformer (<c>--sql</c> or an
    /// explicit processor flag such as <c>--merge</c>).
    /// Stream transformer branches do not receive an injected <c>-i</c> reader — the transformer
    /// reads directly from the upstream Arrow channels.
    /// </summary>
    public bool HasStreamTransformer => ProcessorName != null;

    /// <summary>
    /// Optional pre-parsed job definition if loaded from YAML.
    /// If present, this overrides the logic otherwise derived from <see cref="Arguments"/>.
    /// </summary>
    public Models.JobDefinition? PreParsedJob { get; set; }
}
