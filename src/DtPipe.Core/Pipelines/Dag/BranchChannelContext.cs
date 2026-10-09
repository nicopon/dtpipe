using DtPipe.Core.Abstractions.Dag;

namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Describes the channel wiring provided by the orchestrator for a single branch execution.
/// F5: routing is carried in typed endpoints — the engine never emits CLI flag syntax
/// (<c>-i</c>, <c>-o</c>, <c>mem:</c>, <c>arrow-memory:</c>) for the host to re-parse.
/// </summary>
public record BranchChannelContext
{
    /// <summary>Mapping logical → physical for fan-out sub-channels (stream processors resolve their inputs through it).</summary>
    public IReadOnlyDictionary<string, string> AliasMap { get; init; }
        = new Dictionary<string, string>();

    /// <summary>
    /// Typed input channel endpoint. <see langword="null"/> means the branch reads its own
    /// external <c>-i</c> source (or is a stream processor resolving channels via <see cref="AliasMap"/>).
    /// </summary>
    public InternalChannelEndpoint? InputEndpoint { get; init; }

    /// <summary>
    /// Typed output channel endpoint. <see langword="null"/> means the branch writes to its
    /// own explicit <c>-o</c> target.
    /// </summary>
    public InternalChannelEndpoint? OutputEndpoint { get; init; }

    /// <summary>When <see langword="true"/>, the branch is an intermediate (non-terminal) node and should suppress user-facing stats.</summary>
    public bool SuppressStats { get; init; }

    /// <summary>
    /// The host's fingerprint of what defines this branch's rows, upstream branches included
    /// (see <c>BranchFingerprint</c>). <see langword="null"/> when the host computed none: the
    /// branch then fingerprints itself, which is exact for a branch with no upstream.
    /// </summary>
    public string? DefinitionFingerprint { get; init; }
}
