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
    /// For fan-out branches and stream-transformer branches, the alias of the upstream
    /// branch declared via <c>--from</c>. This triggers a new branch split.
    /// Fan-out: orchestrator injects <c>-i arrow-memory:&lt;alias&gt;</c>.
    /// Stream transformer (SQL/merge): transformer reads directly from the channel.
    /// </summary>
    public string? FromAlias { get; init; }

    /// <summary>
    /// For SQL transformer branches, the aliases of secondary source branches that are
    /// fully preloaded into memory before query execution (declared via <c>--ref</c>).
    /// </summary>
    public IReadOnlyList<string> RefAliases { get; init; } = Array.Empty<string>();

    /// <summary>
    /// For merge transformer branches, the alias of the secondary channel to append
    /// after the main stream (declared via <c>--merge</c>).
    /// </summary>
    public IReadOnlyList<string> MergeAliases { get; init; } = Array.Empty<string>();

    /// <summary>
    /// For SQL transformer branches, the inline SQL query (from <c>--sql "&lt;query&gt;"</c>).
    /// </summary>
    public string? SqlQuery { get; init; }

    /// <summary>
    /// <c>true</c> when this branch activates a stream transformer (<c>--sql</c> or <c>--merge</c>).
    /// Stream transformer branches do not receive an injected <c>-i</c> reader — the transformer
    /// reads directly from the upstream Arrow channels.
    /// </summary>
    public bool HasStreamTransformer => SqlQuery != null || MergeAliases.Count > 0;

    /// <summary>
    /// Optional pre-parsed job definition if loaded from YAML.
    /// If present, this overrides the logic otherwise derived from <see cref="Arguments"/>.
    /// </summary>
    public Models.JobDefinition? PreParsedJob { get; set; }
}
