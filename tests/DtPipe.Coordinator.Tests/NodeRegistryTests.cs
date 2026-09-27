using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using DtPipe.Coordinator.Tests.Infrastructure;
using Xunit;

namespace DtPipe.Coordinator.Tests;

public class NodeRegistryTests
{
    private const string V1 = "v1";

    [Fact]
    public void Register_SameNameFromTheSameConnection_IsIdempotent()
    {
        var registry = new NodeRegistry();
        var clientId = Guid.NewGuid();
        registry.Register(clientId, "conn1", "B", V1);
        registry.Register(clientId, "conn1", "B", V1);

        Assert.Equal("conn1", Assert.Single(registry.GetLiveInstances("B")).ConnectionId);
    }

    /// <summary>
    /// Multiple live instances of one fragment name are now legitimate - redundancy and horizontal
    /// scaling depend on it, and instance alignment (<see cref="AdmissionGate.Resolve"/>) has nothing
    /// to check if a second instance could never register at all.
    /// </summary>
    [Fact]
    public void TwoDifferentClientIds_SameFragmentNameAndVersion_AreBothLiveInstances()
    {
        var registry = new NodeRegistry();
        var client1 = Guid.NewGuid();
        var client2 = Guid.NewGuid();
        registry.Register(client1, "conn1", "A", V1);
        registry.Register(client2, "conn2", "A", V1);

        var live = registry.GetLiveInstances("A");
        Assert.Equal(2, live.Count);
        Assert.Contains(live, n => n.ClientId == client1 && n.ConnectionId == "conn1");
        Assert.Contains(live, n => n.ClientId == client2 && n.ConnectionId == "conn2");
    }

    /// <summary>
    /// Each instance tracks its own grace timer independently: one dropping past grace must not
    /// disturb a second, still-connected instance of the very same fragment name.
    /// </summary>
    [Fact]
    public async Task TwoInstancesOfOneFragmentName_OneDisconnectingPastGrace_DoesNotAffectTheOther()
    {
        var registry = new NodeRegistry(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.FromMilliseconds(100) });
        var client1 = Guid.NewGuid();
        var client2 = Guid.NewGuid();
        registry.Register(client1, "conn1", "A", V1);
        registry.Register(client2, "conn2", "A", V1);

        registry.Unregister("conn1");
        await Task.Delay(300);

        var live = registry.GetLiveInstances("A");
        var survivor = Assert.Single(live);
        Assert.Equal(client2, survivor.ClientId);
    }

    /// <summary>
    /// A version is never forgotten just because every instance that ever hosted it has since
    /// disconnected past grace - pruning is a deferred product decision, not this registry's job. This
    /// is what lets admission tell "never existed" (unknown) apart from "existed, nothing hosts it now"
    /// (retired).
    /// </summary>
    [Fact]
    public async Task HasKnownVersion_RemainsTrueAfterEveryInstanceDisconnectsPastGrace()
    {
        var registry = new NodeRegistry(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.FromMilliseconds(100) });
        registry.Register(Guid.NewGuid(), "conn1", "A", V1);

        registry.Unregister("conn1");
        await Task.Delay(300);

        Assert.Empty(registry.GetLiveInstances("A"));
        Assert.True(registry.HasKnownVersion("A", V1));
        Assert.False(registry.HasKnownVersion("A", "never-seen"));
    }

    /// <summary>
    /// A disconnect and the re-registration that replaces it race by nature. Once a new connection
    /// has reclaimed a fragment name, an <c>Unregister</c> for the old, now-unrelated connection must
    /// not drop the new entry - the deterrent case: a naive <c>Unregister</c> that removes by
    /// fragment name alone orphans the new connection, and its next lookup finds nothing instead of
    /// finding it.
    /// </summary>
    [Fact]
    public void Unregister_ForAConnectionNoLongerOwningTheName_DoesNotOrphanTheNewOwner()
    {
        var registry = new NodeRegistry();
        var clientId1 = Guid.NewGuid();
        registry.Register(clientId1, "conn1", "B", V1);
        registry.Unregister("conn1");

        var clientId2 = Guid.NewGuid();
        registry.Register(clientId2, "conn2", "B", V1);
        registry.Unregister("conn1"); // late or duplicate

        var node = Assert.Single(registry.GetLiveInstances("B"));
        Assert.Equal("conn2", node.ConnectionId);
        Assert.Equal(clientId2, registry.TryGetByConnection("conn2")!.ClientId);
    }

    /// <summary>
    /// Registering the same fragment name from two live connections is no longer refused: it is
    /// exactly what a second, redundant instance registering looks like on the wire.
    /// </summary>
    [Fact]
    public async Task Register_SameFragmentFromTwoLiveConnections_BothBecomeLiveInstances()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });
        await using var a = host.CreateClient();
        await using var b = host.CreateClient();
        await a.ConnectAsync();
        await b.ConnectAsync();

        await a.ControlConnection.InvokeAsync("Register", "B", V1);
        await b.ControlConnection.InvokeAsync("Register", "B", V1);

        var registry = host.Host.Services.GetRequiredService<INodeRegistry>();
        Assert.Equal(2, registry.GetLiveInstances("B").Count);
    }

    /// <summary>
    /// Models the reconnect's Register landing before the old connection's own OnDisconnectedAsync
    /// has run: the entry is still live (no Unregister happened yet), but it is the very same
    /// ClientId asking again under a fresh ConnectionId, not a collision.
    /// </summary>
    [Fact]
    public void Register_SameClientIdFromANewConnection_ReclaimsEvenWhileStillLive()
    {
        var registry = new NodeRegistry();
        var clientId = Guid.NewGuid();
        registry.Register(clientId, "conn1", "B", V1);

        registry.Register(clientId, "conn2", "B", V1);

        Assert.Equal("conn2", Assert.Single(registry.GetLiveInstances("B")).ConnectionId);
    }

    /// <summary>
    /// A live reclaim must drop the old ConnectionId's own index entry, or a query against the dead
    /// connection - conn1 never disconnects in this scenario, it is simply superseded - keeps
    /// resolving to the entry conn2 now owns.
    /// </summary>
    [Fact]
    public void Register_SameClientIdFromANewConnection_DropsTheOldConnectionMapping()
    {
        var registry = new NodeRegistry();
        var clientId = Guid.NewGuid();
        registry.Register(clientId, "conn1", "B", V1);

        registry.Register(clientId, "conn2", "B", V1);

        Assert.Null(registry.TryGetByConnection("conn1"));
        Assert.Equal("conn2", registry.TryGetByConnection("conn2")!.ConnectionId);
    }

    /// <summary>
    /// A stale Unregister for the connection a live reclaim already replaced must not disturb the
    /// new owner - the same race <see cref="Unregister_ForAConnectionNoLongerOwningTheName_DoesNotOrphanTheNewOwner"/>
    /// covers for a grace-period reclaim, here for a live one instead.
    /// </summary>
    [Fact]
    public void Unregister_ForTheConnectionALiveReclaimAlreadyReplaced_DoesNotDisturbTheNewOwner()
    {
        var registry = new NodeRegistry();
        var clientId = Guid.NewGuid();
        registry.Register(clientId, "conn1", "B", V1);
        registry.Register(clientId, "conn2", "B", V1); // a fast reconnect landing before conn1's own disconnect

        registry.Unregister("conn1"); // arrives late

        Assert.Equal("conn2", Assert.Single(registry.GetLiveInstances("B")).ConnectionId);
    }

    [Fact]
    public void GetLiveInstances_DuringGracePeriod_ExcludesTheGracedInstance()
    {
        var registry = new NodeRegistry(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.FromSeconds(30) });
        registry.Register(Guid.NewGuid(), "conn1", "B", V1);

        registry.Unregister("conn1");

        Assert.Empty(registry.GetLiveInstances("B"));
        Assert.Null(registry.TryGetByConnection("conn1"));
    }

    /// <summary>
    /// A same-ClientId reclaim within the grace period cancels the pending loss outright: the
    /// deterrent case for an eager "declare lost on any disconnect" implementation, which would fail
    /// an in-flight run over an ordinary reconnect - the same physical node, so nothing was lost.
    /// </summary>
    [Fact]
    public async Task ASameClientReclaimWithinGrace_NeverFiresFragmentLost()
    {
        var registry = new NodeRegistry(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.FromMilliseconds(150) });
        var lost = new List<string>();
        registry.FragmentLost += (fragment, _) => lost.Add(fragment);
        var clientId = Guid.NewGuid();
        registry.Register(clientId, "conn1", "B", V1);

        registry.Unregister("conn1");
        registry.Register(clientId, "conn2", "B", V1); // the same node reconnecting

        await Task.Delay(300); // past the original 150ms grace deadline

        Assert.Empty(lost);
        Assert.Equal("conn2", Assert.Single(registry.GetLiveInstances("B")).ConnectionId);
    }

    /// <summary>
    /// A *different* ClientId registering the same fragment name while another instance is still
    /// within its own grace period is simply a new, separate instance now - entries are keyed by
    /// (fragment name, ClientId), so this insert cannot touch the graced one. The old identity is left
    /// to time out on its own grace period, exactly like any other drop; there is no longer a
    /// "different ClientId reclaims => declare the old one lost immediately" special case, since a
    /// restarted node's replacement ClientId no longer collides with anything to reclaim.
    /// </summary>
    [Fact]
    public async Task ADifferentClientRegisteringWhileAnotherIsGraced_DoesNotCancelTheOldsGrace()
    {
        var registry = new NodeRegistry(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.FromMilliseconds(150) });
        var lost = new List<(string Fragment, Guid ClientId)>();
        registry.FragmentLost += (fragment, id) => lost.Add((fragment, id));
        var oldClientId = Guid.NewGuid();
        registry.Register(oldClientId, "conn1", "B", V1);

        registry.Unregister("conn1");
        var newClientId = Guid.NewGuid();
        registry.Register(newClientId, "conn2", "B", V1); // a distinct, additional instance - not a reclaim

        Assert.Empty(lost); // the old instance is still within its own grace, untouched by this arrival
        Assert.Equal([newClientId], registry.GetLiveInstances("B").Select(n => n.ClientId));

        await Task.Delay(300); // past the old instance's own 150ms grace deadline
        var entry = Assert.Single(lost);
        Assert.Equal("B", entry.Fragment);
        Assert.Equal(oldClientId, entry.ClientId);
        Assert.Equal([newClientId], registry.GetLiveInstances("B").Select(n => n.ClientId));
    }

    [Fact]
    public async Task AGracePeriodThatElapsesWithNoReclaim_FiresFragmentLostAndDropsTheEntry()
    {
        var registry = new NodeRegistry(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.FromMilliseconds(100) });
        var clientId = Guid.NewGuid();
        var lost = new List<(string Fragment, Guid ClientId)>();
        registry.FragmentLost += (fragment, id) => lost.Add((fragment, id));
        registry.Register(clientId, "conn1", "B", V1);

        registry.Unregister("conn1");
        await Task.Delay(400);

        var entry = Assert.Single(lost);
        Assert.Equal("B", entry.Fragment);
        Assert.Equal(clientId, entry.ClientId);
        Assert.Empty(registry.GetLiveInstances("B"));
    }

    [Fact]
    public void AZeroGracePeriod_DeclaresLostImmediately()
    {
        var registry = new NodeRegistry(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.Zero });
        var clientId = Guid.NewGuid();
        var lost = new List<string>();
        registry.FragmentLost += (fragment, _) => lost.Add(fragment);
        registry.Register(clientId, "conn1", "B", V1);

        registry.Unregister("conn1");

        Assert.Equal(["B"], lost);
        Assert.Empty(registry.GetLiveInstances("B"));
    }
}
