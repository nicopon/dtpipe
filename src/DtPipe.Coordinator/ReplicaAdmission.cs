using DtPipe.Coordinator.Contracts;
using TransportR.Hub;

namespace DtPipe.Coordinator;

/// <summary>
/// One fragment a run needs. <paramref name="Node"/> is the key of the node that may host it: the subject its
/// replicas connect under, which the caller reads from the rights and the library never infers (for a hub that
/// attests no subject, the name its replicas announce). <paramref name="Groups"/> are the groups the rights give that
/// node today, which its replica's TransportR client must hold exactly. <paramref name="Cursor"/> is the value the fragment's source starts
/// from, sent with its launch and no part of its version.
/// </summary>
public sealed record RunFragment(FragmentSpec Spec, string? Node, IReadOnlyList<string>? Groups = null, CursorLaunch? Cursor = null)
{
    public string Name => Spec.Fragment;
}

/// <summary>What <see cref="ReplicaAdmission"/> and <see cref="ReplicaProtocol"/> read: how a fragment bound to no identity is taken, and the protection required of a replica.</summary>
public sealed class AdmissionOptions
{
    /// <summary>
    /// A hub that attests no subject (development, tests with no token): a replica that announces none is filed under
    /// the name it proposes, and a fragment that names no groups is not compared with any. A host with an identity
    /// provider never sets it. Off by default: a fragment bound to no identity is refused.
    /// </summary>
    public bool AdmitUnboundFragments { get; init; }

    private readonly string _requiredProtection = DataPlaneProtections.None;

    /// <summary>
    /// The level of protection of the data plane every replica must announce to be eligible, a word of <see cref="ProtectionPolicy.Words"/>
    /// (any case is read as the word): the protection of a replica below it is no ground to refuse its announcement, it stays connected
    /// and listed, and it is chosen for no fragment (<see cref="ReplicaChooser.ProtectionRefusal"/>). The weakest level, the default,
    /// requires nothing.
    /// </summary>
    /// <exception cref="ArgumentException">The text is none of the words.</exception>
    public string RequiredProtection
    {
        get => _requiredProtection;
        init => _requiredProtection = ProtectionPolicy.Read(value)
            ?? throw new ArgumentException("A required protection is one of the words of ProtectionPolicy.Words.", nameof(value));
    }
}

/// <summary>
/// A run naming a fragment bound to no identity: nothing says which node may host it, and a hub that attests subjects
/// admits no replica on that footing (see <see cref="AdmissionOptions"/>).
/// </summary>
public sealed class UnboundFragmentException : Exception
{
    public string FragmentName { get; }

    public UnboundFragmentException(string fragmentName)
        : base($"Run refused, fragment '{fragmentName}' names no node identity or groups: nothing says which replica may host it.")
    {
        FragmentName = fragmentName;
    }
}

/// <summary>
/// Admission refused a run: when the wait for a replica ran out, at least one fragment had none that could run it, or every
/// fragment had one and no choice of them joined the two ends of every edge. <see cref="Reasons"/> names each such fragment and
/// says why, replica by replica, or, when every fragment had a candidate, names an edge (<see cref="EdgeProtection.Name"/>) and
/// what each of its ends does to the data plane.
/// </summary>
public sealed class NoEligibleReplicaException : Exception
{
    public IReadOnlyDictionary<string, string> Reasons { get; }

    public NoEligibleReplicaException(IReadOnlyDictionary<string, string> reasons)
        : base("Run refused, no replica can run: " + string.Join("; ", reasons.Select(r => $"{r.Key} ({r.Value})")))
    {
        Reasons = reasons;
    }
}

/// <summary>Which replicas could run a fragment right now, or why none can.</summary>
public sealed record FragmentEligibility(IReadOnlyList<ReplicaKey> Eligible, string? Reason);

/// <summary>
/// Decides which replica runs each fragment of a run. A replica is eligible for a fragment when it is connected, of
/// the fragment's node, its TransportR client is registered under the right subject with the right groups
/// (<see cref="ReplicaChooser.DataPlaneRefusal"/>), the bricks it announces compose the version of the plan
/// (<see cref="ReplicaChooser.CompositionRefusal"/>), and it protects the data plane at least as the deployment requires
/// (<see cref="ReplicaChooser.ProtectionRefusal"/>). Eligibility is a fragment's; whether two replicas can carry an edge
/// is the edge's (<see cref="EdgeProtection"/>). Among the eligible ones the registry takes, for every fragment together,
/// the one with the fewest runs among those that let the two ends of every edge join.
/// </summary>
public sealed class ReplicaAdmission
{
    private readonly ReplicaRegistry _registry;
    private readonly IStateStore _state;
    private readonly AdmissionOptions _options;

    public ReplicaAdmission(ReplicaRegistry registry, IStateStore state, AdmissionOptions? options = null)
    {
        _registry = registry;
        _state = state;
        _options = options ?? new AdmissionOptions();
    }

    /// <summary>A fragment bound to no node or to no groups is refused before any wait, unless the hub attests no subject.</summary>
    /// <exception cref="UnboundFragmentException"/>
    public void ThrowIfUnbound(IReadOnlyList<RunFragment> fragments)
    {
        foreach (var fragment in fragments)
            if (fragment.Node is null || (fragment.Groups is null && !_options.AdmitUnboundFragments))
                throw new UnboundFragmentException(fragment.Name);
    }

    private sealed record Registrations(Dictionary<(ReplicaKey, string?, Guid?), ClientRegistration?> Read);

    private async Task<(IReadOnlyList<ReplicaView> Replicas, Registrations Registrations)> ReadAsync(IReadOnlyList<RunFragment> fragments)
    {
        var nodes = fragments.Select(f => f.Node).ToHashSet();
        var replicas = _registry.Replicas().Where(r => nodes.Contains(r.Key.Node)).ToList();
        var read = new Dictionary<(ReplicaKey, string?, Guid?), ClientRegistration?>();
        foreach (var replica in replicas.Where(r => r.Connected))
            read[(replica.Key, replica.ConnectionId, replica.DataPlaneClientId)] =
                replica.DataPlaneClientId is { } id ? await _state.GetClientAsync(id) : null;
        return (replicas, new Registrations(read));
    }

    private string? Refusal(RunFragment fragment, ReplicaView replica, Registrations registrations)
    {
        if (!registrations.Read.TryGetValue((replica.Key, replica.ConnectionId, replica.DataPlaneClientId), out var registration))
            return "it changed while it was read.";
        return ReplicaChooser.ProtectionRefusal(replica, _options.RequiredProtection)
            ?? ReplicaChooser.DataPlaneRefusal(replica, registration, fragment.Groups, _options.AdmitUnboundFragments)
            ?? ReplicaChooser.CompositionRefusal(fragment.Spec, replica.Announcement);
    }

    private static string Explain(RunFragment fragment, IReadOnlyList<ReplicaView> replicas, Func<ReplicaView, string?> refusal)
    {
        var connected = replicas.Where(r => r.Key.Node == fragment.Node && r.Connected).ToList();
        if (connected.Count == 0) return "no replica of its node is online.";
        return string.Join(" ", connected.Select(r => $"replica {r.Key.Short}: {refusal(r)}"));
    }

    /// <summary>
    /// For each fragment, the replicas that could run it now, or why none can. Reserves nothing, and judges no edge: an eligible
    /// replica may still be one no run takes because its protection of the data plane joins no end of an edge
    /// (<see cref="EdgeRefusals"/> at arming, <see cref="TryReserveAsync"/> when a run is placed).
    /// </summary>
    public async Task<IReadOnlyDictionary<string, FragmentEligibility>> EvaluateAsync(IReadOnlyList<RunFragment> fragments)
    {
        var (replicas, registrations) = await ReadAsync(fragments);
        var result = new Dictionary<string, FragmentEligibility>(StringComparer.Ordinal);
        foreach (var fragment in fragments)
        {
            var eligible = replicas.Where(r => r.Key.Node == fragment.Node && r.Connected && Refusal(fragment, r, registrations) is null)
                .Select(r => r.Key).ToList();
            result[fragment.Name] = new FragmentEligibility(eligible, eligible.Count > 0 ? null : Explain(fragment, replicas, r => Refusal(fragment, r, registrations)));
        }
        return result;
    }

    /// <summary>
    /// Places every fragment of <paramref name="runId"/> on a replica in one step, as <see cref="ReplicaRegistry.TryReserve"/>
    /// does, from what the replicas and TransportR say now, so that the two ends of each of <paramref name="edges"/> join. An
    /// edge pairs the fragments that the run wires together, its producer and its consumer, whatever their nodes. When none is
    /// placed, <c>Reasons</c> says why for each fragment that has no candidate, or, when each has one, names the edges no choice
    /// of them joins (<see cref="EdgeProtection.Refusals"/>).
    /// </summary>
    public async Task<(IReadOnlyDictionary<string, ReplicaView>? Chosen, IReadOnlyDictionary<string, string> Reasons)> TryReserveAsync(
        string runId, IReadOnlyList<RunFragment> fragments, IReadOnlyList<RunEdge>? edges = null)
    {
        var (replicas, registrations) = await ReadAsync(fragments);
        var byName = fragments.ToDictionary(f => f.Name, StringComparer.Ordinal);
        var pairs = (edges ?? []).Select(e => (e.ProducerFragment, e.ConsumerFragment)).ToList();
        var chosen = _registry.TryReserve(runId, fragments.Select(f => (f.Name, f.Node!)).ToList(),
            (replica, fragment) => Refusal(byName[fragment], replica, registrations) is null, pairs);
        if (chosen is not null) return (chosen, new Dictionary<string, string>());

        var reasons = new Dictionary<string, string>(StringComparer.Ordinal);
        var offers = new List<IReadOnlyCollection<ReplicaProtection>>();
        foreach (var fragment in fragments)
        {
            var candidates = replicas.Where(r => r.Key.Node == fragment.Node && r.Connected && Refusal(fragment, r, registrations) is null).ToList();
            if (candidates.Count == 0) reasons[fragment.Name] = Explain(fragment, replicas, r => Refusal(fragment, r, registrations));
            offers.Add(candidates.Select(ReplicaProtection.Of).ToHashSet());
        }
        if (reasons.Count > 0) return (null, reasons);
        // Every fragment has a candidate: what keeps the run from being placed is an edge whose ends no choice of them joins.
        var names = fragments.Select(f => f.Name).ToList();
        return (null, EdgeProtection.Refusals(names, offers, EdgeProtection.Indexed(names, pairs)));
    }

    /// <summary>
    /// The edges no pair of replicas connected now could carry, a sentence each: <c>edge P -&gt; C: P seals with key …, C sends in
    /// clear.</c> An edge is refused only when the nodes of its two fragments each have a connected replica and none of one joins any
    /// of the other (<see cref="ReplicaProtection.Joins"/>); a node with no connected replica is not judged here, and admission judges
    /// it when a run is placed. A replica below the required protection is eligible for nothing, so it joins nothing and is no end
    /// of a pair: a node whose connected replicas are all below it is a node with no replica to judge, and admission names it. Replicas
    /// that differ within a node pass while they offer a joining pair, as a rolling update does; pairs are judged edge by edge, so only
    /// admission's joint choice decides a run. Reserves nothing.
    /// </summary>
    public IReadOnlyList<string> EdgeRefusals(IReadOnlyList<RunFragment> fragments, IReadOnlyList<RunEdge> edges)
    {
        var nodes = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (var fragment in fragments) nodes.TryAdd(fragment.Name, fragment.Node);
        var connected = _registry.Replicas().Where(r => r.Connected && !ProtectionPolicy.IsBelow(r, _options.RequiredProtection)).ToList();
        List<ReplicaProtection> Offers(string fragment) => nodes.GetValueOrDefault(fragment) is { } node
            ? connected.Where(r => r.Key.Node == node).Select(ReplicaProtection.Of).Distinct().ToList()
            : [];

        var refusals = new List<string>();
        foreach (var edge in edges.DistinctBy(e => (e.ProducerFragment, e.ConsumerFragment)))
        {
            var (producer, consumer) = (Offers(edge.ProducerFragment), Offers(edge.ConsumerFragment));
            if (producer.Count == 0 || consumer.Count == 0 || producer.Any(p => consumer.Any(p.Joins))) continue;
            refusals.Add($"{EdgeProtection.Name(edge.ProducerFragment, edge.ConsumerFragment)}: " +
                $"{EdgeProtection.Ends(edge.ProducerFragment, producer, edge.ConsumerFragment, consumer)}.");
        }
        return refusals;
    }
}
