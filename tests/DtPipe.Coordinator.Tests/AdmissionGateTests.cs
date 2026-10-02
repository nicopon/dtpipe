using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Coordinator.Tests.Infrastructure;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// <see cref="AdmissionGate.Resolve"/> as a pure function - every branch of the versioning rule,
/// without a hub, a registry or a single poll. <see cref="AdmissionGateTests"/> below covers the same
/// rule end to end, against the real hub and <see cref="NodeRegistry"/> a
/// <see cref="CoordinatorHub.Register"/> call populates.
/// </summary>
public class AdmissionGateResolveTests
{
    private static RegisteredNode Node(string fragment, string version, Guid? clientId = null) =>
        new(clientId ?? Guid.NewGuid(), "conn", fragment, version);

    [Fact]
    public void NoLiveInstances_Unpinned_IsAbsent()
    {
        var result = AdmissionGate.Resolve(new FragmentPin("A"), [], _ => false);
        Assert.Equal(new PinRefused(PinRefusalReason.Absent), result);
    }

    [Fact]
    public void NoLiveInstances_PinnedToAVersionNeverSeen_IsUnknown()
    {
        var result = AdmissionGate.Resolve(new FragmentPin("A", "v1"), [], _ => false);
        Assert.Equal(new PinRefused(PinRefusalReason.Unknown), result);
    }

    [Fact]
    public void NoLiveInstances_PinnedToAVersionOnceSeen_IsRetired()
    {
        var result = AdmissionGate.Resolve(new FragmentPin("A", "v1"), [], _ => true);
        Assert.Equal(new PinRefused(PinRefusalReason.Retired), result);
    }

    [Fact]
    public void OneLiveVersion_Unpinned_Resolves()
    {
        var node = Node("A", "v1");
        var result = AdmissionGate.Resolve(new FragmentPin("A"), [node], _ => false);
        Assert.Equal(new PinResolved(node), result);
    }

    [Fact]
    public void OneLiveVersion_PinMatches_Resolves()
    {
        var node = Node("A", "v1");
        var result = AdmissionGate.Resolve(new FragmentPin("A", "v1"), [node], _ => false);
        Assert.Equal(new PinResolved(node), result);
    }

    [Fact]
    public void OneLiveVersion_PinMismatch_KnownElsewhere_IsRetired()
    {
        var node = Node("A", "v2");
        var result = AdmissionGate.Resolve(new FragmentPin("A", "v1"), [node], v => v == "v1");
        Assert.Equal(new PinRefused(PinRefusalReason.Retired), result);
    }

    [Fact]
    public void OneLiveVersion_PinMismatch_NeverSeen_IsUnknown()
    {
        var node = Node("A", "v2");
        var result = AdmissionGate.Resolve(new FragmentPin("A", "v1"), [node], _ => false);
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
        var result = AdmissionGate.Resolve(
            new FragmentPin("A", "v1"), [Node("A", "v2"), Node("A", "v1")], _ => false);

        // Not a whole-record Assert.Equal: PinMisaligned's synthesized equality compares Versions by
        // reference (List<string> does not override Equals), so two structurally-equal lists built
        // separately would never compare equal that way.
        var misaligned = Assert.IsType<PinMisaligned>(result);
        Assert.Equal(["v1", "v2"], misaligned.Versions);
    }
}

/// <summary>
/// The rendez-vous barrier: a run starts only once every fragment it names has registered, at the
/// version it asked for. <see cref="AdmissionGate"/> is exercised here against the real hub and
/// <see cref="NodeRegistry"/> singleton a <see cref="CoordinatorHub.Register"/> call populates, not a
/// fake.
/// </summary>
public class AdmissionGateTests
{
    private static readonly TimeSpan ShortGrace = TimeSpan.FromMilliseconds(100);

    [Fact]
    public async Task AllFragmentsRegistered_IsAdmitted()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });

        await using var a = host.CreateClient();
        await using var b = host.CreateClient();
        await a.ConnectAsync();
        await b.ConnectAsync();
        await a.ControlConnection.InvokeAsync("Register", "fragment-a", "v1");
        await b.ControlConnection.InvokeAsync("Register", "fragment-b", "v1");

        var gate = host.Host.Services.GetRequiredService<AdmissionGate>();
        var admitted = await gate.AwaitAllAsync(
            [new FragmentPin("fragment-a"), new FragmentPin("fragment-b")], TimeSpan.FromSeconds(5));

        Assert.Equal(["fragment-a", "fragment-b"], admitted.Select(n => n.FragmentName).Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task AnAbsentFragment_RefusesTheRun_NamingIt()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });

        await using var a = host.CreateClient();
        await a.ConnectAsync();
        await a.ControlConnection.InvokeAsync("Register", "fragment-a", "v1");

        var gate = host.Host.Services.GetRequiredService<AdmissionGate>();
        var ex = await Assert.ThrowsAsync<AdmissionRefusedException>(
            () => gate.AwaitAllAsync(
                [new FragmentPin("fragment-a"), new FragmentPin("fragment-b")], TimeSpan.FromMilliseconds(200)));

        Assert.Contains("fragment-b", ex.AbsentFragments);
        Assert.DoesNotContain("fragment-a", ex.AbsentFragments);
    }

    [Fact]
    public async Task ARegistrationArrivingLate_StillAdmitsBeforeTheTimeout()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });

        await using var a = host.CreateClient();
        await using var b = host.CreateClient();
        await a.ConnectAsync();
        await b.ConnectAsync();
        await a.ControlConnection.InvokeAsync("Register", "fragment-a", "v1");

        var gate = host.Host.Services.GetRequiredService<AdmissionGate>();
        var admission = gate.AwaitAllAsync(
            [new FragmentPin("fragment-a"), new FragmentPin("fragment-b")], TimeSpan.FromSeconds(5));

        await Task.Delay(100);
        await b.ControlConnection.InvokeAsync("Register", "fragment-b", "v1");

        var admitted = await admission.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(2, admitted.Count);
    }

    /// <summary>
    /// A requester's own cancellation is a different caller than a timeout: reporting it as
    /// <see cref="AdmissionRefusedException"/> would blame absent fragments for a wait the caller
    /// itself gave up on, which <see cref="RunOrchestrator"/> relies on to tell "requester cancelled"
    /// apart from "admission timed out" - the two map to different run outcomes.
    /// </summary>
    [Fact]
    public async Task ARequesterCancellation_ThrowsOperationCanceled_NeverAdmissionRefused()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });
        var gate = host.Host.Services.GetRequiredService<AdmissionGate>();

        using var cts = new CancellationTokenSource();
        var admission = gate.AwaitAllAsync([new FragmentPin("fragment-a")], TimeSpan.FromSeconds(30), cts.Token);
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => admission);
    }

    /// <summary>
    /// Today's code cannot even represent two live instances of one fragment name (the pre-lot
    /// <c>NodeRegistry</c> refuses the second live <c>Register</c> outright) - this is the guard that
    /// only becomes expressible once instance alignment exists at all.
    /// </summary>
    [Fact]
    public async Task TwoLiveInstances_DisagreeingOnVersion_ThrowsMisalignedInstances_NamingFragmentAndBothVersions()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });

        await using var a = host.CreateClient();
        await using var b = host.CreateClient();
        await a.ConnectAsync();
        await b.ConnectAsync();
        await a.ControlConnection.InvokeAsync("Register", "fragment-a", "v1");
        await b.ControlConnection.InvokeAsync("Register", "fragment-a", "v2");

        var gate = host.Host.Services.GetRequiredService<AdmissionGate>();
        var ex = await Assert.ThrowsAsync<MisalignedInstancesException>(
            () => gate.AwaitAllAsync([new FragmentPin("fragment-a")], TimeSpan.FromMilliseconds(200)));

        Assert.Equal("fragment-a", ex.FragmentName);
        Assert.Equal(["v1", "v2"], ex.Versions.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task PinnedToAVersionNeverRegistered_ThrowsPinnedVersionUnavailable_Unknown()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });

        await using var a = host.CreateClient();
        await a.ConnectAsync();
        await a.ControlConnection.InvokeAsync("Register", "fragment-a", "v1");

        var gate = host.Host.Services.GetRequiredService<AdmissionGate>();
        var ex = await Assert.ThrowsAsync<PinnedVersionUnavailableException>(
            () => gate.AwaitAllAsync([new FragmentPin("fragment-a", "v-ghost")], TimeSpan.FromMilliseconds(200)));

        Assert.Equal("fragment-a", ex.FragmentName);
        Assert.Equal("v-ghost", ex.Version);
        Assert.Equal(PinRefusalReason.Unknown, ex.Reason);
    }

    /// <summary>
    /// The literal plan guard: a run pinned to a version that was registered once but nothing hosts
    /// anymore is refused, naming it - distinct from a version nobody ever reported at all.
    /// </summary>
    [Fact]
    public async Task PinnedToARetiredVersion_ThrowsPinnedVersionUnavailable_Retired()
    {
        await using var host = await CoordinatorTestHost.StartAsync(
            _ => { }, services => services.AddSingleton(new NodeRegistryOptions { DisconnectGracePeriod = ShortGrace }));

        var a = host.CreateClient();
        await a.ConnectAsync();
        await a.ControlConnection.InvokeAsync("Register", "fragment-a", "v1");
        await a.DisconnectAsync();
        await a.DisposeAsync();
        await Task.Delay(300); // past ShortGrace - nothing hosts v1 anymore

        await using var b = host.CreateClient();
        await b.ConnectAsync();
        await b.ControlConnection.InvokeAsync("Register", "fragment-a", "v2");

        var gate = host.Host.Services.GetRequiredService<AdmissionGate>();
        var ex = await Assert.ThrowsAsync<PinnedVersionUnavailableException>(
            () => gate.AwaitAllAsync([new FragmentPin("fragment-a", "v1")], TimeSpan.FromMilliseconds(200)));

        Assert.Equal("fragment-a", ex.FragmentName);
        Assert.Equal("v1", ex.Version);
        Assert.Equal(PinRefusalReason.Retired, ex.Reason);
    }

    /// <summary>
    /// Proves there is no separate "rollback" mechanism: v1 is retired, then re-registered by a brand
    /// new instance once v2's own instance has in turn gone the same way. Pinning to v1 succeeds
    /// because it is, again, the one live version - identical to any ordinary pin match, nothing
    /// version-history-aware about it.
    /// </summary>
    [Fact]
    public async Task RollbackIsFree_PinningToAVersionThatIsAgainTheOnlyLiveOne_Succeeds()
    {
        await using var host = await CoordinatorTestHost.StartAsync(
            _ => { }, services => services.AddSingleton(new NodeRegistryOptions { DisconnectGracePeriod = ShortGrace }));

        var a = host.CreateClient();
        await a.ConnectAsync();
        await a.ControlConnection.InvokeAsync("Register", "fragment-a", "v1");
        await a.DisconnectAsync();
        await a.DisposeAsync();
        await Task.Delay(300); // v1 retired - nothing hosts it

        var b = host.CreateClient();
        await b.ConnectAsync();
        await b.ControlConnection.InvokeAsync("Register", "fragment-a", "v2");
        await b.DisconnectAsync();
        await b.DisposeAsync();
        await Task.Delay(300); // v2 retired too - nothing hosts it either

        await using var c = host.CreateClient();
        await c.ConnectAsync();
        await c.ControlConnection.InvokeAsync("Register", "fragment-a", "v1"); // a new instance, hosting v1 again

        var gate = host.Host.Services.GetRequiredService<AdmissionGate>();
        var admitted = await gate.AwaitAllAsync([new FragmentPin("fragment-a", "v1")], TimeSpan.FromSeconds(5));

        Assert.Equal("v1", Assert.Single(admitted).Version);
    }
}
