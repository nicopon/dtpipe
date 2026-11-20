using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator;

/// <summary>
/// The level of protection of the data plane a deployment may require of every replica, and the one order the levels are judged by.
/// A replica announcing a level below the required one stays connected and listed and is eligible for no fragment
/// (<see cref="ReplicaChooser.ProtectionRefusal"/>). The policy reads words of <see cref="DataPlaneProtections"/> through
/// <see cref="ReplicaView.DataPlaneProtection"/>, never a key or a key id: whether two ends hold the same key is the edge's
/// (<see cref="ReplicaProtection.Joins"/>).
/// </summary>
public static class ProtectionPolicy
{
    // Weakest first: the rank of a level is its place.
    private static readonly string[] Levels = [DataPlaneProtections.None, DataPlaneProtections.Encrypted, DataPlaneProtections.EncryptedPsk];

    /// <summary>The words a deployment may require, weakest first.</summary>
    public static IReadOnlyList<string> Words => Levels;

    /// <summary>The word <paramref name="text"/> names, whatever its case, or null when it is none of <see cref="Words"/>.</summary>
    public static string? Read(string? text) => Levels.FirstOrDefault(word => string.Equals(word, text, StringComparison.OrdinalIgnoreCase));

    /// <summary>Whether <paramref name="replica"/> announces a level weaker than <paramref name="required"/>, a word of <see cref="Words"/>.</summary>
    public static bool IsBelow(ReplicaView replica, string required) => Rank(replica.DataPlaneProtection) < Rank(required);

    /// <summary>
    /// What a replica below <paramref name="required"/> does and what the deployment requires, as the predicate of a sentence:
    /// <c>sends in clear and this deployment requires encrypted-psk</c>. Words of levels only: no key id, whatever the replica announced.
    /// </summary>
    public static string Shortfall(ReplicaView replica, string required) =>
        $"{new ReplicaProtection(replica.DataPlaneProtection, null).Words} and this deployment requires {required}";

    private static int Rank(string level) => Array.IndexOf(Levels, DataPlaneProtections.Read(level));
}
