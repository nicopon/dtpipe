using System.Text.Json;

namespace DtPipe.Coordinator.Server;

/// <summary>Why a pipeline's placement does not hold, as <see cref="PlacementProblem.Cause"/>.</summary>
public static class PlacementCauses
{
    /// <summary>A branch is a brick and the placement names no node for it.</summary>
    public const string Unassigned = "unassigned";

    /// <summary>The placement names a node the coordinator knows no identity for.</summary>
    public const string UnknownNode = "unknown-node";

    /// <summary>The placement names a node that has no connected replica.</summary>
    public const string Offline = "offline";

    /// <summary>The placement names a node that does not offer the brick the branch is.</summary>
    public const string NotOffered = "not-offered";

    /// <summary>The placement names a branch that is no brick: only a brick has a node, the runner is never named.</summary>
    public const string NotABrick = "not-a-brick";

    /// <summary>The placement names a branch the job does not have.</summary>
    public const string NoBranch = "no-branch";
}

/// <summary>
/// One reason a placement does not hold for a job. <see cref="Blocking"/>: the job cannot be distributed or deployed
/// as it stands; a node that is only offline does not block, since its replicas come back.
/// </summary>
public sealed record PlacementProblem(string Alias, string Cause, string Message, bool Blocking);

/// <summary>
/// Where a pipeline's bricks run: the node each source or sink branch of its job is placed on, by the branch's alias.
/// The runner is never named: it is stateless, and every branch that is no brick goes to it. The library keeps it beside the
/// job, in <c>placement.json</c>, so the job stays a plain dtpipe job.
/// </summary>
public static class Placement
{
    private static readonly JsonSerializerOptions Json = new() { WriteIndented = true };

    public static string Serialize(IReadOnlyDictionary<string, string> placement) =>
        JsonSerializer.Serialize(new SortedDictionary<string, string>(placement.ToDictionary(p => p.Key, p => p.Value), StringComparer.Ordinal), Json);

    /// <summary>The hash a saved plan keeps of the placement it was made from; null for none, so an empty placement and no file are the same.</summary>
    public static string? Sha(IReadOnlyDictionary<string, string> placement) =>
        placement.Count == 0 ? null : PipelineLibrary.Sha(Serialize(placement));
}
