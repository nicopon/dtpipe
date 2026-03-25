namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Describes the channel wiring provided by the orchestrator for a single branch execution.
/// The CLI layer reads <see cref="ChannelInjection"/> to route the branch to the correct
/// in-memory channels without the engine having to know CLI flag syntax.
/// </summary>
public record BranchChannelContext
{
    /// <summary>Mapping logical → physical for fan-out sub-channels.</summary>
    public IReadOnlyDictionary<string, string> AliasMap { get; init; }
        = new Dictionary<string, string>();

    /// <summary>
    /// Channel routing plan set by <see cref="DagOrchestrator"/> for branches that read from
    /// or write to in-memory channels. <see langword="null"/> for linear (non-DAG) branches.
    /// </summary>
    public ChannelInjectionPlan? ChannelInjection { get; init; }
}
