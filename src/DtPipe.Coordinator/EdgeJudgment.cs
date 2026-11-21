using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator;

/// <summary>
/// What the replicas connected now say of one edge of a plan, by the rule of <see cref="EdgeProtection"/>. It holds words only: a level
/// of <see cref="DataPlaneProtections"/>, the sentence of a refusal (which names key fingerprints, never a key) and the names of the two
/// fragments.
/// <para>
/// <paramref name="Protection"/> is the level the edge would be held at, when every pair of its ends' offers that joins is of that one
/// level. <paramref name="Refusal"/> is the sentence of <see cref="EdgeProtection.Judge"/> when both ends have offers and no pair joins.
/// An edge with neither is not decided, and is never read as one in clear: an end with no offer (no connected replica, or every one
/// below the required protection) leaves it unknown, and so does a pair of ends that join at several levels, which replicas that differ
/// within a node allow. A level of <see cref="DataPlaneProtections.None"/> is a known level.
/// </para>
/// </summary>
public sealed record EdgeJudgment(string ProducerFragment, string ConsumerFragment, string? Protection, string? Refusal)
{
    /// <summary>The refusals among <paramref name="judgments"/>, in their order.</summary>
    public static IReadOnlyList<string> RefusalsOf(IEnumerable<EdgeJudgment> judgments) =>
        [.. judgments.Select(j => j.Refusal).OfType<string>()];
}
