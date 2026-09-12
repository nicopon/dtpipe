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
    public Models.JobDefinition? PreParsedJob { get; init; }

    /// <summary>
    /// The one projection from a hydrated <see cref="Models.JobDefinition"/> to the branch the
    /// orchestrator runs — CLI arguments, CLI <c>--job</c>, the YAML path behind MCP and the
    /// linear path's synthetic branch all come through here.
    /// </summary>
    /// <remarks>
    /// Four call sites used to spell this out by hand and the copies drifted: two of them left
    /// <see cref="PreParsedJob"/> unset, so <c>get-dag-topology</c> promised a branch's
    /// transformers without emitting them and the CLI panel listed branches with no stages while
    /// the results table below it showed those stages running. Reintroducing a hand-written
    /// initializer reopens that class of defect.
    /// </remarks>
    /// <param name="alias">The branch's key in the job dictionary.</param>
    /// <param name="job">The hydrated job this branch runs.</param>
    /// <param name="processorFactories">
    /// Catalogue consulted to name the branch's stream processor. Ignored when
    /// <paramref name="processorName"/> is supplied.
    /// </param>
    /// <param name="arguments">The CLI slice that defined the branch; empty on the YAML paths.</param>
    /// <param name="processorName">
    /// Supplied by the CLI arguments path, which resolves the processor from the raw tokens
    /// (<c>--sql</c>, <c>--merge</c>) before the job carries the provider options to detect it.
    /// </param>
    public static BranchDefinition FromJob(
        string alias,
        Models.JobDefinition job,
        IEnumerable<Abstractions.IStreamTransformerFactory>? processorFactories = null,
        string[]? arguments = null,
        string? processorName = null) => new()
    {
        Alias = alias,
        Input = job.Input,
        Output = job.Output,
        StreamingAliases = SplitAliases(job.From),
        RefAliases = job.Ref ?? Array.Empty<string>(),
        Arguments = arguments ?? Array.Empty<string>(),
        ProcessorName = processorName
            ?? processorFactories?.FirstOrDefault(f => f.IsApplicable(job))?.ComponentName,
        PreParsedJob = job
    };

    /// <summary>
    /// <c>from</c> carries the aliases comma-separated; absent means no upstream, which is the
    /// absence of the key rather than one empty alias.
    /// </summary>
    private static IReadOnlyList<string> SplitAliases(string? from)
        => string.IsNullOrEmpty(from)
            ? Array.Empty<string>()
            : from.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
}
