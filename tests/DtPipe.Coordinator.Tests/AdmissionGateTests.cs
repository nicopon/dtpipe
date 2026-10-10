using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// <see cref="AdmissionGate.Resolve"/> as a pure function - every branch of the versioning rule,
/// without a hub, a registry or a single poll.
/// </summary>
public class AdmissionGateResolveTests
{
    private static RegisteredNode Node(string fragment, string version, Guid? clientId = null, string? subject = null) =>
        new(clientId ?? Guid.NewGuid(), "conn", fragment, version, subject);

    /// <summary>The version and alignment rules, on pins and instances that name no subject (a development host).</summary>
    private static PinResolution ResolveUnbound(FragmentPin pin, IReadOnlyList<RegisteredNode> live, Func<string, bool> hasKnownVersion) =>
        AdmissionGate.Resolve(pin, live, hasKnownVersion, admitUnbound: true);

    [Fact]
    public void NoLiveInstances_Unpinned_IsAbsent()
    {
        var result = ResolveUnbound(new FragmentPin("A"), [], _ => false);
        Assert.Equal(new PinRefused(PinRefusalReason.Absent), result);
    }

    [Fact]
    public void NoLiveInstances_PinnedToAVersionNeverSeen_IsUnknown()
    {
        var result = ResolveUnbound(new FragmentPin("A", "v1"), [], _ => false);
        Assert.Equal(new PinRefused(PinRefusalReason.Unknown), result);
    }

    [Fact]
    public void NoLiveInstances_PinnedToAVersionOnceSeen_IsRetired()
    {
        var result = ResolveUnbound(new FragmentPin("A", "v1"), [], _ => true);
        Assert.Equal(new PinRefused(PinRefusalReason.Retired), result);
    }

    [Fact]
    public void OneLiveVersion_Unpinned_Resolves()
    {
        var node = Node("A", "v1");
        var result = ResolveUnbound(new FragmentPin("A"), [node], _ => false);
        Assert.Equal(new PinResolved(node), result);
    }

    [Fact]
    public void OneLiveVersion_PinMatches_Resolves()
    {
        var node = Node("A", "v1");
        var result = ResolveUnbound(new FragmentPin("A", "v1"), [node], _ => false);
        Assert.Equal(new PinResolved(node), result);
    }

    [Fact]
    public void OneLiveVersion_PinMismatch_KnownElsewhere_IsRetired()
    {
        var node = Node("A", "v2");
        var result = ResolveUnbound(new FragmentPin("A", "v1"), [node], v => v == "v1");
        Assert.Equal(new PinRefused(PinRefusalReason.Retired), result);
    }

    [Fact]
    public void OneLiveVersion_PinMismatch_NeverSeen_IsUnknown()
    {
        var node = Node("A", "v2");
        var result = ResolveUnbound(new FragmentPin("A", "v1"), [node], _ => false);
        Assert.Equal(new PinRefused(PinRefusalReason.Unknown), result);
    }

    /// <summary>
    /// Instance alignment is checked unconditionally, even for a pinned request that names one of the
    /// two disagreeing versions: a misaligned pair is a defect of the peer itself, never resolved by
    /// what was asked for.
    /// </summary>
    [Fact]
    public void TwoDistinctLiveVersions_IsMisaligned_EvenWhenPinnedToOneOfThem()
    {
        var result = ResolveUnbound(
            new FragmentPin("A", "v1"), [Node("A", "v2"), Node("A", "v1")], _ => false);

        // Not a whole-record Assert.Equal: PinMisaligned's synthesized equality compares Versions by
        // reference (List<string> does not override Equals), so two structurally-equal lists built
        // separately would never compare equal that way.
        var misaligned = Assert.IsType<PinMisaligned>(result);
        Assert.Equal(["v1", "v2"], misaligned.Versions);
    }

    /// <summary>
    /// An instance connected under another subject is nobody's peer for this fragment: its version is
    /// not compared with the right instance's, so it can neither refuse the run nor be counted.
    /// </summary>
    [Fact]
    public void AnInstanceOfAnotherSubject_WithAnotherVersion_IsNeverCounted()
    {
        var legit = Node("A", "v1", subject: "node-a");
        var foreign = Node("A", "v-other", subject: "node-b");

        var result = AdmissionGate.Resolve(new FragmentPin("A", Subject: "node-a"), [foreign, legit], _ => false);

        Assert.Equal(new PinResolved(legit), result);
    }

    [Fact]
    public void AnInstanceOfAnotherSubject_ListedFirst_IsNeverPicked()
    {
        var legit = Node("A", "v1", subject: "node-a");
        var foreign = Node("A", "v1", subject: "node-b");

        var result = AdmissionGate.Resolve(new FragmentPin("A", Subject: "node-a"), [foreign, legit], _ => false);

        Assert.Equal(new PinResolved(legit), result);
    }

    [Fact]
    public void OnlyAnInstanceOfAnotherSubject_IsAbsent()
    {
        var result = AdmissionGate.Resolve(
            new FragmentPin("A", Subject: "node-a"), [Node("A", "v1", subject: "node-b")], _ => false);

        Assert.Equal(new PinRefused(PinRefusalReason.Absent), result);
    }

    [Fact]
    public void AnInstanceThatAttestsNoSubject_NeverServesASubjectPin()
    {
        var result = AdmissionGate.Resolve(new FragmentPin("A", Subject: "node-a"), [Node("A", "v1")], _ => false);

        Assert.Equal(new PinRefused(PinRefusalReason.Absent), result);
    }

    [Fact]
    public void APinThatNamesNoSubject_ServesNoInstance_UnlessUnboundFragmentsAreAdmitted()
    {
        var node = Node("A", "v1", subject: "node-a");

        Assert.Equal(new PinRefused(PinRefusalReason.Absent), AdmissionGate.Resolve(new FragmentPin("A"), [node], _ => false));
        Assert.Equal(new PinResolved(node), AdmissionGate.Resolve(new FragmentPin("A"), [node], _ => false, admitUnbound: true));
    }
}
