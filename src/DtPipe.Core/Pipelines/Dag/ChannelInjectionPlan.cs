using DtPipe.Core.Abstractions.Dag;

namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Carries channel routing information from <see cref="DagOrchestrator"/> to the branch executor
/// callback in the CLI layer. Decouples the engine from CLI flag syntax (<c>-i</c>, <c>-o</c>,
/// <c>mem:</c>, <c>arrow-memory:</c>, <c>--no-stats</c>).
/// </summary>
public record ChannelInjectionPlan
{
    /// <summary>
    /// Alias of the branch to use as input.
    /// <see langword="null"/> means the branch has an explicit <c>-i</c>.
    /// </summary>
    public string? InputChannelAlias { get; init; }

    /// <summary>
    /// Alias of the branch to use as output.
    /// <see langword="null"/> means the branch has an explicit <c>-o</c>.
    /// </summary>
    public string? OutputChannelAlias { get; init; }

    /// <summary>
    /// When <see langword="true"/>, the branch is an intermediate (non-terminal) node and
    /// should suppress user-facing stats output.
    /// </summary>
    public bool SuppressStats { get; init; }
}
