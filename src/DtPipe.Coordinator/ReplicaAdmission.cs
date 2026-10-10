using DtPipe.Coordinator.Contracts;
using TransportR.Interfaces;
using TransportR.Models;

namespace DtPipe.Coordinator;

/// <summary>
/// One fragment a run needs. <paramref name="Node"/> is the key of the node that may host it: the subject its
/// replicas connect under, which the caller reads from the rights and the library never infers (for a hub that
/// attests no subject, the name its replicas announce). <paramref name="Groups"/> are the groups the rights give that
/// node today, which its replica's TransportR client must hold exactly.
/// </summary>
public sealed record RunFragment(FragmentSpec Spec, string? Node, IReadOnlyList<string>? Groups = null)
{
    public string Name => Spec.Fragment;
}

/// <summary>What <see cref="ReplicaAdmission"/> reads as a fragment bound to no identity.</summary>
public sealed class AdmissionOptions
{
    /// <summary>
    /// A hub that attests no subject (development, tests with no token): a replica that announces none is filed under
    /// the name it proposes, and a fragment that names no groups is not compared with any. A host with an identity
    /// provider never sets it. Off by default: a fragment bound to no identity is refused.
    /// </summary>
    public bool AdmitUnboundFragments { get; init; }
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
/// Admission refused a run: when the wait for a replica ran out, at least one fragment had none that could run it.
/// <see cref="Reasons"/> names each such fragment and says why, replica by replica.
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
/// (<see cref="ReplicaChooser.DataPlaneRefusal"/>), and the bricks it announces compose the version of the plan
/// (<see cref="ReplicaChooser.CompositionRefusal"/>). Among the eligible ones the registry takes the one with the
/// fewest runs.
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
        return ReplicaChooser.DataPlaneRefusal(replica, registration, fragment.Groups, _options.AdmitUnboundFragments)
            ?? ReplicaChooser.CompositionRefusal(fragment.Spec, replica.Announcement);
    }

    private static string Explain(RunFragment fragment, IReadOnlyList<ReplicaView> replicas, Func<ReplicaView, string?> refusal)
    {
        var connected = replicas.Where(r => r.Key.Node == fragment.Node && r.Connected).ToList();
        if (connected.Count == 0) return "no replica of its node is online.";
        return string.Join(" ", connected.Select(r => $"replica {r.Key.Short}: {refusal(r)}"));
    }

    /// <summary>For each fragment, the replicas that could run it now, or why none can. Reserves nothing.</summary>
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
    /// does, from what the replicas and TransportR say now. When none is placed, <c>Reasons</c> says why for each
    /// fragment that has no candidate.
    /// </summary>
    public async Task<(IReadOnlyDictionary<string, ReplicaView>? Chosen, IReadOnlyDictionary<string, string> Reasons)> TryReserveAsync(
        string runId, IReadOnlyList<RunFragment> fragments)
    {
        var (replicas, registrations) = await ReadAsync(fragments);
        var byName = fragments.ToDictionary(f => f.Name, StringComparer.Ordinal);
        var chosen = _registry.TryReserve(runId, fragments.Select(f => (f.Name, f.Node!)).ToList(),
            (replica, fragment) => Refusal(byName[fragment], replica, registrations) is null);
        if (chosen is not null) return (chosen, new Dictionary<string, string>());

        var reasons = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var fragment in fragments)
        {
            var hasCandidate = replicas.Any(r => r.Key.Node == fragment.Node && r.Connected && Refusal(fragment, r, registrations) is null);
            if (!hasCandidate) reasons[fragment.Name] = Explain(fragment, replicas, r => Refusal(fragment, r, registrations));
        }
        return (null, reasons);
    }
}
