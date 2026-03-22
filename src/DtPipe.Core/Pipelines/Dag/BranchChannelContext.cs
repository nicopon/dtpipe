namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Describes the channel wiring injected by the orchestrator for a single branch execution.
/// Passed to the branch executor callback but typically used only for informational purposes —
/// the actual channel injection happens via CLI arg manipulation in DagOrchestrator.
/// </summary>
public record BranchChannelContext
{
    /// <summary>Mapping logical → physical for fan-out sub-channels.</summary>
    public IReadOnlyDictionary<string, string> AliasMap { get; init; }
        = new Dictionary<string, string>();
}
