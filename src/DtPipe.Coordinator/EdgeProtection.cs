using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator;

/// <summary>
/// What one end of an edge does to the bytes of its transfers: a level of <see cref="DataPlaneProtections"/> and the fingerprint of
/// its pre-shared key, read from a replica through <see cref="ReplicaView.DataPlaneProtection"/> and <see cref="ReplicaView.KeyId"/>,
/// never through the text it announced.
/// </summary>
public readonly record struct ReplicaProtection(string Level, string? KeyId)
{
    public static ReplicaProtection Of(ReplicaView replica) => new(replica.DataPlaneProtection, replica.KeyId);

    /// <summary>
    /// Whether a transfer between this end and <paramref name="other"/> can open: the same level and, where both name a key id, the
    /// same one. An end that names none (a key per peer) joins every end of its level, so the relation is not transitive.
    /// </summary>
    public bool Joins(ReplicaProtection other) =>
        Level == other.Level && (KeyId is null || other.KeyId is null || KeyId == other.KeyId);

    /// <summary>What the end does, as a refusal says it. The key id is written whole: two fingerprints must never read alike.</summary>
    public string Words => Level switch
    {
        DataPlaneProtections.Encrypted => "seals without a key",
        DataPlaneProtections.EncryptedPsk => KeyId is null ? "seals with a key per peer" : $"seals with key {KeyId}",
        _ => "sends in clear",
    };
}

/// <summary>
/// The rule the two ends of an edge answer to. TransportR has no downgrade: a transfer whose ends protect the data plane differently
/// fails before its first batch, so the coordinator places a run only where the ends of every edge join
/// (<see cref="ReplicaProtection.Joins"/>), comparing words and fingerprints, never a key. Eligibility is a fragment's
/// (<see cref="ReplicaChooser"/>); joining is an edge's, and since the relation is not transitive the replicas of a run are chosen
/// together (<see cref="ReplicaRegistry.TryReserve"/>), never fragment by fragment.
/// </summary>
public static class EdgeProtection
{
    /// <summary>How a refusal names an edge.</summary>
    public static string Name(string producer, string consumer) => $"edge {producer} -> {consumer}";

    /// <summary>What each end of an edge does, from the protections it can take: "P seals with key …, C sends in clear".</summary>
    public static string Ends(
        string producer, IEnumerable<ReplicaProtection> producerOffers, string consumer, IEnumerable<ReplicaProtection> consumerOffers) =>
        $"{producer} {Words(producerOffers)}, {consumer} {Words(consumerOffers)}";

    private static string Words(IEnumerable<ReplicaProtection> offers) =>
        string.Join(" or ", offers.Select(o => o.Words).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal));

    /// <summary>The edges between <paramref name="fragments"/>, by their positions; an edge naming a fragment of no position is left out.</summary>
    internal static IReadOnlyList<(int Producer, int Consumer)> Indexed(
        IReadOnlyList<string> fragments, IEnumerable<(string Producer, string Consumer)> edges)
    {
        var at = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < fragments.Count; i++) at.TryAdd(fragments[i], i);
        return [.. edges.Where(e => at.ContainsKey(e.Producer) && at.ContainsKey(e.Consumer)).Select(e => (at[e.Producer], at[e.Consumer]))];
    }

    /// <summary><paramref name="offers"/> with the fragment at <paramref name="fragment"/> held to <paramref name="protection"/>.</summary>
    internal static IReadOnlyList<IReadOnlyCollection<ReplicaProtection>> With(
        IReadOnlyList<IReadOnlyCollection<ReplicaProtection>> offers, int fragment, ReplicaProtection protection) =>
        [.. offers.Select((o, i) => i == fragment ? [protection] : o)];

    /// <summary>
    /// Whether a protection can be taken for every fragment among its <paramref name="offers"/> so that the two ends of every edge join.
    /// The edges split the fragments into connected parts, and every fragment of a part takes one level. At that level, a fragment that
    /// can take a protection naming no key joins all its neighbours and binds none of them; the others, joined by edges among themselves,
    /// share one key. Linear in the fragments, their offers and the edges.
    /// </summary>
    internal static bool Joinable(IReadOnlyList<IReadOnlyCollection<ReplicaProtection>> offers, IReadOnlyList<(int Producer, int Consumer)> edges)
    {
        if (offers.Any(o => o.Count == 0)) return false;
        var parts = new Groups(offers.Count);
        foreach (var (producer, consumer) in edges) parts.Join(producer, consumer);
        foreach (var part in Enumerable.Range(0, offers.Count).GroupBy(parts.Find))
        {
            var members = part.ToList();
            var inner = edges.Where(e => parts.Find(e.Producer) == part.Key).ToList();
            if (!offers[members[0]].Select(o => o.Level).Distinct().Any(level => JoinableAt(level, members, offers, inner))) return false;
        }
        return true;
    }

    private static bool JoinableAt(
        string level, IReadOnlyList<int> members, IReadOnlyList<IReadOnlyCollection<ReplicaProtection>> offers, IReadOnlyList<(int Producer, int Consumer)> edges)
    {
        if (members.Any(m => !offers[m].Any(o => o.Level == level))) return false;
        var keyed = members.Where(m => !offers[m].Any(o => o.Level == level && o.KeyId is null)).ToHashSet();
        var groups = new Groups(offers.Count);
        foreach (var (producer, consumer) in edges)
            if (keyed.Contains(producer) && keyed.Contains(consumer)) groups.Join(producer, consumer);
        return keyed.GroupBy(groups.Find).All(group => group
            .Select(m => offers[m].Where(o => o.Level == level).Select(o => o.KeyId!).ToHashSet(StringComparer.Ordinal))
            .Aggregate((shared, keys) => { shared.IntersectWith(keys); return shared; })
            .Count > 0);
    }

    /// <summary>
    /// Why no choice among <paramref name="offers"/> joins the ends of every edge, by the name of the edge: each edge, in the run's order,
    /// that cannot be joined together with the edges before it that could, and what its two ends can take once those are joined (an edge
    /// no pair of its ends' offers joins is named whatever comes before it). Empty when a choice joins them all.
    /// </summary>
    internal static IReadOnlyDictionary<string, string> Refusals(
        IReadOnlyList<string> fragments, IReadOnlyList<IReadOnlyCollection<ReplicaProtection>> offers, IReadOnlyList<(int Producer, int Consumer)> edges)
    {
        var reasons = new Dictionary<string, string>(StringComparer.Ordinal);
        var joined = new List<(int Producer, int Consumer)>();
        foreach (var edge in edges)
        {
            if (Joinable(offers, [.. joined, edge]))
            {
                joined.Add(edge);
                continue;
            }
            var producer = offers[edge.Producer].Where(o => Joinable(With(offers, edge.Producer, o), joined)).ToList();
            var consumer = offers[edge.Consumer].Where(o => Joinable(With(offers, edge.Consumer, o), joined)).ToList();
            var narrowed = producer.Count < offers[edge.Producer].Count || consumer.Count < offers[edge.Consumer].Count;
            reasons.TryAdd(
                Name(fragments[edge.Producer], fragments[edge.Consumer]),
                Ends(fragments[edge.Producer], producer, fragments[edge.Consumer], consumer) + (narrowed ? ", as the edges before it leave them" : ""));
        }
        return reasons;
    }

    /// <summary>Which fragments the edges have joined so far: each set is named by one of its members.</summary>
    private sealed class Groups(int count)
    {
        private readonly int[] _parent = [.. Enumerable.Range(0, count)];

        public int Find(int member)
        {
            while (_parent[member] != member) member = _parent[member] = _parent[_parent[member]];
            return member;
        }

        public void Join(int a, int b) => _parent[Find(a)] = Find(b);
    }
}
