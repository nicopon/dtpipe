namespace DtPipe.Coordinator;

/// <summary>A fragment a run needs, optionally pinned to a version - null is Docker's own "latest": whatever a live instance currently reports.</summary>
public sealed record FragmentPin(string FragmentName, string? Version = null);

/// <summary>Admission refused a run: at least one unpinned fragment never registered at all (no live instance, any version).</summary>
public sealed class AdmissionRefusedException : Exception
{
    public IReadOnlyList<string> AbsentFragments { get; }

    public AdmissionRefusedException(IReadOnlyList<string> absentFragments)
        : base("Run refused, fragment(s) never registered: " + string.Join(", ", absentFragments))
    {
        AbsentFragments = absentFragments;
    }
}

/// <summary>
/// A run pinned to a fragment version with no live instance behind it - <see cref="PinRefusalReason.Unknown"/>
/// when that version was never registered by anyone, <see cref="PinRefusalReason.Retired"/> when it
/// was but nothing hosts it anymore (the free "rollback" is simply pinning to a version that still,
/// or again, has a live instance - there is no separate mechanism).
/// </summary>
public sealed class PinnedVersionUnavailableException : Exception
{
    public string FragmentName { get; }
    public string Version { get; }
    public PinRefusalReason Reason { get; }

    public PinnedVersionUnavailableException(string fragmentName, string version, PinRefusalReason reason)
        : base(reason == PinRefusalReason.Unknown
            ? $"Run refused, fragment '{fragmentName}' has no known version '{version}'."
            : $"Run refused, fragment '{fragmentName}' pinned to version '{version}', but no live instance currently hosts it.")
    {
        FragmentName = fragmentName;
        Version = version;
        Reason = reason;
    }
}

/// <summary>
/// A run refused because two or more live instances of one fragment name disagree on version -
/// checked unconditionally, even for a pinned request: instance misalignment is a defect of the peer
/// itself, independent of what the run asked for.
/// </summary>
public sealed class MisalignedInstancesException : Exception
{
    public string FragmentName { get; }
    public IReadOnlyList<string> Versions { get; }

    public MisalignedInstancesException(string fragmentName, IReadOnlyList<string> versions)
        : base($"Run refused, fragment '{fragmentName}' instances disagree on version: {string.Join(", ", versions)}.")
    {
        FragmentName = fragmentName;
        Versions = versions;
    }
}

/// <summary>
/// Absent: unpinned, nothing at all hosts this fragment. Unknown/Retired: pinned, told apart by
/// whether the version was ever seen. Public (unlike the rest of the pure resolution surface below)
/// because <see cref="PinnedVersionUnavailableException.Reason"/> exposes it across the assembly
/// boundary.
/// </summary>
public enum PinRefusalReason { Absent, Unknown, Retired }

internal abstract record PinResolution;
internal sealed record PinResolved(RegisteredNode Node) : PinResolution;
internal sealed record PinRefused(PinRefusalReason Reason) : PinResolution;
internal sealed record PinMisaligned(IReadOnlyList<string> Versions) : PinResolution;

/// <summary>
/// The rendez-vous barrier: a run starts only once every fragment it names has registered, at the
/// version it asked for. No lot launches a single child ahead of the others - an absent peer, an
/// unavailable pin, or a misaligned pair of instances is each a named, hard refusal, not a stuck lone
/// consumer.
/// </summary>
public sealed class AdmissionGate
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(20);

    private readonly INodeRegistry _registry;

    public AdmissionGate(INodeRegistry registry)
    {
        _registry = registry;
    }

    /// <summary>
    /// Pure. Instance alignment is checked first and unconditionally, even for a pinned request: two
    /// live instances of one name disagreeing on version is a defect of the peer, never resolved by
    /// what was asked for. Otherwise, with a single agreed live version (or none at all): an unpinned
    /// pin resolves to that version, or is refused as <see cref="PinRefusalReason.Absent"/> when no
    /// instance exists; a pinned request resolves only on an exact match, and otherwise is refused,
    /// named unknown or retired via <paramref name="hasKnownVersion"/>.
    /// </summary>
    internal static PinResolution Resolve(
        FragmentPin pin, IReadOnlyList<RegisteredNode> liveInstances, Func<string, bool> hasKnownVersion)
    {
        if (liveInstances.Count == 0)
        {
            if (pin.Version is null) return new PinRefused(PinRefusalReason.Absent);
            return new PinRefused(hasKnownVersion(pin.Version) ? PinRefusalReason.Retired : PinRefusalReason.Unknown);
        }

        var distinctVersions = liveInstances
            .Select(n => n.Version)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(v => v, StringComparer.Ordinal)
            .ToList();
        if (distinctVersions.Count > 1)
            return new PinMisaligned(distinctVersions);

        var liveVersion = distinctVersions[0];
        if (pin.Version is null || pin.Version == liveVersion)
            return new PinResolved(liveInstances[0]);

        return new PinRefused(hasKnownVersion(pin.Version) ? PinRefusalReason.Retired : PinRefusalReason.Unknown);
    }

    /// <exception cref="MisalignedInstancesException">A fragment's live instances disagree on version - checked before any pin refusal.</exception>
    /// <exception cref="PinnedVersionUnavailableException">A pinned fragment's version is unknown or retired.</exception>
    /// <exception cref="AdmissionRefusedException">
    /// At least one unpinned fragment in <paramref name="pins"/> still had no live instance at all
    /// when <paramref name="timeout"/> elapsed. Names every one still missing at that instant.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="ct"/> itself fired - a requester cancellation, never reported as a named
    /// refusal: the two are different callers and a caller that cancelled its own wait did not
    /// have any fragment refuse it.
    /// </exception>
    public async Task<IReadOnlyList<RegisteredNode>> AwaitAllAsync(
        IReadOnlyList<FragmentPin> pins, TimeSpan timeout, CancellationToken ct = default)
    {
        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

        while (true)
        {
            var resolved = pins
                .Select(pin => (Pin: pin, Resolution: Resolve(
                    pin, _registry.GetLiveInstances(pin.FragmentName), v => _registry.HasKnownVersion(pin.FragmentName, v))))
                .ToList();

            if (resolved.All(r => r.Resolution is PinResolved))
                return resolved.Select(r => ((PinResolved)r.Resolution).Node).ToList();

            ct.ThrowIfCancellationRequested();

            if (timeoutCts.IsCancellationRequested)
            {
                foreach (var (pin, resolution) in resolved)
                    if (resolution is PinMisaligned misaligned)
                        throw new MisalignedInstancesException(pin.FragmentName, misaligned.Versions);

                foreach (var (pin, resolution) in resolved)
                    if (resolution is PinRefused { Reason: PinRefusalReason.Unknown or PinRefusalReason.Retired } refused)
                        throw new PinnedVersionUnavailableException(pin.FragmentName, pin.Version!, refused.Reason);

                var absent = resolved
                    .Where(r => r.Resolution is PinRefused { Reason: PinRefusalReason.Absent })
                    .Select(r => r.Pin.FragmentName)
                    .ToList();
                throw new AdmissionRefusedException(absent);
            }

            try { await Task.Delay(PollInterval, linked.Token); }
            catch (OperationCanceledException) { /* loop once more to tell which side fired above */ }
        }
    }
}
