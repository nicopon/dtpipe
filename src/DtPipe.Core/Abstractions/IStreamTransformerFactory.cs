using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Factory for creating <see cref="IStreamTransformer"/> instances from CLI arguments.
/// Registered alongside reader/writer/transformer factories so the CLI can detect
/// stream-transformer branches without a special "processor" concept.
/// </summary>
public interface IStreamTransformerFactory
{
    /// <summary>Human-readable name, e.g. "sql" or "merge".</summary>
    string ComponentName { get; }

    /// <summary>
    /// When <c>true</c>, upstream branches that feed into this transformer must use
    /// Arrow memory channels (RecordBatch) rather than native object[] channels.
    /// </summary>
    bool RequiresArrowChannels { get; }

    /// <summary>
    /// Returns <c>true</c> if the given branch arguments indicate that this transformer
    /// should be activated (e.g. <c>--sql</c> present for SqlTransformerFactory).
    /// </summary>
    bool IsApplicable(string[] branchArgs);

    /// <summary>
    /// Creates the transformer from the branch arguments.
    /// <paramref name="ctx"/> carries the logical→physical alias map built by the orchestrator
    /// for fan-out scenarios; implementations use it to resolve physical channel aliases
    /// for registry lookup without needing to know the internal <c>__fan_N</c> convention.
    /// </summary>
    IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider serviceProvider);
}
