using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using DtPipe.Coordinator.Tests.Infrastructure;
using Xunit;

namespace DtPipe.Coordinator.Tests;

public class NodeRegistryTests
{
    [Fact]
    public void Register_SameNameFromADifferentConnection_Throws()
    {
        var registry = new NodeRegistry();
        registry.Register(Guid.NewGuid(), "conn1", "B");

        var ex = Assert.Throws<InvalidOperationException>(() => registry.Register(Guid.NewGuid(), "conn2", "B"));
        Assert.Contains("B", ex.Message);
    }

    [Fact]
    public void Register_SameNameFromTheSameConnection_IsIdempotent()
    {
        var registry = new NodeRegistry();
        var clientId = Guid.NewGuid();
        registry.Register(clientId, "conn1", "B");
        registry.Register(clientId, "conn1", "B");

        Assert.Equal("conn1", registry.TryGetByFragment("B")!.ConnectionId);
    }

    /// <summary>
    /// A disconnect and the re-registration that replaces it race by nature. Once a new connection
    /// has reclaimed a fragment name, an <c>Unregister</c> for the old, now-unrelated connection must
    /// not drop the new entry - the deterrent case: a naive <c>Unregister</c> that removes by
    /// fragment name alone orphans the new connection, and its next lookup throws
    /// <see cref="KeyNotFoundException"/> instead of finding it.
    /// </summary>
    [Fact]
    public void Unregister_ForAConnectionNoLongerOwningTheName_DoesNotOrphanTheNewOwner()
    {
        var registry = new NodeRegistry();
        registry.Register(Guid.NewGuid(), "conn1", "B");
        registry.Unregister("conn1");

        var clientId2 = Guid.NewGuid();
        registry.Register(clientId2, "conn2", "B");
        registry.Unregister("conn1"); // late or duplicate

        var node = registry.TryGetByFragment("B");
        Assert.NotNull(node);
        Assert.Equal("conn2", node!.ConnectionId);
        Assert.Equal(clientId2, registry.TryGetByConnection("conn2")!.ClientId);
    }

    [Fact]
    public async Task Register_SameFragmentFromTwoLiveConnections_RefusesTheSecond()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });
        await using var a = host.CreateClient();
        await using var b = host.CreateClient();
        await a.ConnectAsync();
        await b.ConnectAsync();

        await a.ControlConnection.InvokeAsync("Register", "B");

        var ex = await Assert.ThrowsAsync<HubException>(
            () => b.ControlConnection.InvokeAsync("Register", "B"));
        Assert.Contains("B", ex.Message);
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
        registry.Register(clientId, "conn1", "B");

        registry.Register(clientId, "conn2", "B");

        Assert.Equal("conn2", registry.TryGetByFragment("B")!.ConnectionId);
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
        registry.Register(clientId, "conn1", "B");

        registry.Register(clientId, "conn2", "B");

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
        registry.Register(clientId, "conn1", "B");
        registry.Register(clientId, "conn2", "B"); // a fast reconnect landing before conn1's own disconnect

        registry.Unregister("conn1"); // arrives late

        Assert.Equal("conn2", registry.TryGetByFragment("B")!.ConnectionId);
    }

    [Fact]
    public void TryGetByFragment_DuringGracePeriod_IsNull()
    {
        var registry = new NodeRegistry(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.FromSeconds(30) });
        registry.Register(Guid.NewGuid(), "conn1", "B");

        registry.Unregister("conn1");

        Assert.Null(registry.TryGetByFragment("B"));
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
        registry.Register(clientId, "conn1", "B");

        registry.Unregister("conn1");
        registry.Register(clientId, "conn2", "B"); // the same node reconnecting

        await Task.Delay(300); // past the original 150ms grace deadline

        Assert.Empty(lost);
        Assert.Equal("conn2", registry.TryGetByFragment("B")!.ConnectionId);
    }

    /// <summary>
    /// A *different* ClientId reclaiming a pending entry - a restarted process, not a reconnect of
    /// the same one - fires FragmentLost for the old identity immediately: the grace timer this
    /// reclaim cancels was its only path there, so an in-flight run holding the old ClientId would
    /// otherwise never learn it is gone.
    /// </summary>
    [Fact]
    public async Task ADifferentClientReclaimWithinGrace_FiresFragmentLostForTheOldIdentity()
    {
        var registry = new NodeRegistry(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.FromMilliseconds(150) });
        var lost = new List<(string Fragment, Guid ClientId)>();
        registry.FragmentLost += (fragment, id) => lost.Add((fragment, id));
        var oldClientId = Guid.NewGuid();
        registry.Register(oldClientId, "conn1", "B");

        registry.Unregister("conn1");
        var newClientId = Guid.NewGuid();
        registry.Register(newClientId, "conn2", "B"); // a different physical process, same fragment name

        var entry = Assert.Single(lost);
        Assert.Equal("B", entry.Fragment);
        Assert.Equal(oldClientId, entry.ClientId);

        await Task.Delay(300); // past the original 150ms grace deadline - no second event from expiry
        Assert.Single(lost);
        Assert.Equal(newClientId, registry.TryGetByFragment("B")!.ClientId);
    }

    [Fact]
    public async Task AGracePeriodThatElapsesWithNoReclaim_FiresFragmentLostAndDropsTheEntry()
    {
        var registry = new NodeRegistry(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.FromMilliseconds(100) });
        var clientId = Guid.NewGuid();
        var lost = new List<(string Fragment, Guid ClientId)>();
        registry.FragmentLost += (fragment, id) => lost.Add((fragment, id));
        registry.Register(clientId, "conn1", "B");

        registry.Unregister("conn1");
        await Task.Delay(400);

        var entry = Assert.Single(lost);
        Assert.Equal("B", entry.Fragment);
        Assert.Equal(clientId, entry.ClientId);
        Assert.Null(registry.TryGetByFragment("B"));
    }

    [Fact]
    public void AZeroGracePeriod_DeclaresLostImmediately()
    {
        var registry = new NodeRegistry(new NodeRegistryOptions { DisconnectGracePeriod = TimeSpan.Zero });
        var clientId = Guid.NewGuid();
        var lost = new List<string>();
        registry.FragmentLost += (fragment, _) => lost.Add(fragment);
        registry.Register(clientId, "conn1", "B");

        registry.Unregister("conn1");

        Assert.Equal(["B"], lost);
        Assert.Null(registry.TryGetByFragment("B"));
    }
}
