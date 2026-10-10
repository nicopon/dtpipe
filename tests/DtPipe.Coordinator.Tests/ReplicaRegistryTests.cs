using DtPipe.Coordinator.Contracts;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// The one table of the replicas that announced themselves: presence and its grace period, the same host taking its
/// place back, a replica leaving, and how a run's fragments are placed on the replicas of their nodes.
/// </summary>
public class ReplicaRegistryTests
{
    private static NodeAnnouncement Announcement(string name, Guid? instance = null, Guid? client = null, string? origin = null) =>
        new(name, "", [], NodeRole.Runner, [], InstanceId: instance, Origin: origin, DataPlaneClientId: client);

    private static Guid Instance(int n) => Guid.Parse($"00000000-0000-0000-0000-{n:000000000000}");

    private static ReplicaKey Announce(ReplicaRegistry registry, string connection, string node, Guid? instance = null, Action? abort = null, Guid? client = null) =>
        registry.Announce(connection, node, Announcement(node, instance, client), ["g"], abort ?? (() => { }));

    [Fact]
    public void TwoInstancesOfOneNode_AreTwoReplicas_AndTheSameInstanceIsOne()
    {
        var registry = new ReplicaRegistry();

        var first = Announce(registry, "c1", "n", Instance(1));
        var second = Announce(registry, "c2", "n", Instance(2));
        var again = Announce(registry, "c3", "n", Instance(1));

        Assert.NotEqual(first, second);
        Assert.Equal(first, again);
        Assert.Equal(2, registry.Replicas("n").Count);
        Assert.Equal("c3", registry.ConnectionOf(first));
    }

    [Fact]
    public void ANodeKeysItsReplicasByTheSubject_NeverByTheNameAReplicaProposes()
    {
        var registry = new ReplicaRegistry();

        registry.Announce("c1", "subject-a", Announcement("shared-name", Instance(1)), [], () => { });
        registry.Announce("c2", "subject-b", Announcement("shared-name", Instance(1)), [], () => { });

        Assert.Single(registry.Replicas("subject-a"));
        Assert.Single(registry.Replicas("subject-b"));
        Assert.Empty(registry.Replicas("shared-name"));
    }

    [Fact]
    public void TheSameHostOnANewConnection_TakesItsPlaceBack_AndTheOldConnectionIsClosed()
    {
        var registry = new ReplicaRegistry();
        var closed = 0;
        var key = Announce(registry, "c1", "n", Instance(1), () => closed++);
        registry.TryReserve("run-1", [("f", "n")], (_, _) => true);
        var since = registry.Replicas("n").Single().FirstAnnouncedAt;

        Announce(registry, "c2", "n", Instance(1));

        Assert.Equal(1, closed);
        var replica = registry.Replicas("n").Single();
        Assert.Equal("c2", replica.ConnectionId);
        Assert.Equal(since, replica.FirstAnnouncedAt);
        Assert.Single(replica.Leases);
        Assert.Null(registry.OfConnection("c1"));
        Assert.Equal(key, registry.OfConnection("c2")!.Key);
    }

    [Fact]
    public void TheLateCloseOfAReplacedConnection_ChangesNothing()
    {
        var registry = new ReplicaRegistry();
        var lost = new List<ReplicaKey>();
        registry.ReplicaLost += lost.Add;
        Announce(registry, "c1", "n", Instance(1));
        Announce(registry, "c2", "n", Instance(1));

        registry.Disconnected("c1");

        Assert.True(registry.Replicas("n").Single().Connected);
        Assert.Empty(lost);
    }

    [Fact]
    public async Task ADroppedConnection_IsReclaimedInsideItsGrace_AndLostOnlyOutsideIt()
    {
        var registry = new ReplicaRegistry(new ReplicaRegistryOptions { DisconnectGracePeriod = TimeSpan.FromMilliseconds(300) });
        var lost = new List<ReplicaKey>();
        registry.ReplicaLost += lost.Add;
        var reclaimed = Announce(registry, "c1", "n", Instance(1));
        var gone = Announce(registry, "c2", "n", Instance(2));

        registry.Disconnected("c1");
        registry.Disconnected("c2");
        Assert.Null(registry.ConnectionOf(reclaimed));
        Assert.DoesNotContain(registry.Replicas("n"), r => r.Connected);
        Announce(registry, "c3", "n", Instance(1));
        await Task.Delay(600);

        Assert.Equal([gone], lost);
        Assert.Equal([reclaimed], registry.Replicas("n").Select(r => r.Key));
    }

    [Fact]
    public void AZeroGracePeriod_LosesAtOnce()
    {
        var registry = new ReplicaRegistry(new ReplicaRegistryOptions { DisconnectGracePeriod = TimeSpan.Zero });
        var lost = new List<ReplicaKey>();
        registry.ReplicaLost += lost.Add;
        var key = Announce(registry, "c1", "n", Instance(1));

        registry.Disconnected("c1");

        Assert.Equal([key], lost);
        Assert.Empty(registry.Replicas("n"));
    }

    [Fact]
    public async Task AReplicaThatLeaves_IsGoneAtOnce_AndItsDisconnectionAfterwardsDeclaresNothingMore()
    {
        var registry = new ReplicaRegistry(new ReplicaRegistryOptions { DisconnectGracePeriod = TimeSpan.FromMilliseconds(200) });
        var lost = new List<ReplicaKey>();
        registry.ReplicaLost += lost.Add;
        var key = Announce(registry, "c1", "n", Instance(1));
        Announce(registry, "c2", "n", Instance(2));

        registry.Leave("c1");
        registry.Disconnected("c1");
        await Task.Delay(400);

        Assert.Equal([key], lost);
        Assert.Single(registry.Replicas("n"));
    }

    [Fact]
    public void ALeaveFromAConnectionThatNoLongerOwnsTheReplica_IsIgnored()
    {
        var registry = new ReplicaRegistry();
        var lost = new List<ReplicaKey>();
        registry.ReplicaLost += lost.Add;
        Announce(registry, "c1", "n", Instance(1));
        Announce(registry, "c2", "n", Instance(1));

        registry.Leave("c1");

        Assert.Empty(lost);
        Assert.True(registry.Replicas("n").Single().Connected);
    }

    [Fact]
    public void Forget_DropsTheReplicasThatAreNotConnectedAsLost_AndKeepsTheConnectedOnes()
    {
        var registry = new ReplicaRegistry();
        var lost = new List<ReplicaKey>();
        registry.ReplicaLost += lost.Add;
        Announce(registry, "c1", "n", Instance(1));
        var graced = Announce(registry, "c2", "n", Instance(2));
        registry.Disconnected("c2");

        registry.Forget("n");

        Assert.Equal([graced], lost);
        Assert.Equal(Instance(1).ToString("N"), registry.Replicas("n").Single().Key.Instance);
    }

    [Fact]
    public void Disconnect_ClosesTheConnectionOfEveryReplicaOfTheNode_AndOnlyThem()
    {
        var registry = new ReplicaRegistry();
        var closed = new List<string>();
        Announce(registry, "c1", "n", Instance(1), () => closed.Add("c1"));
        Announce(registry, "c2", "n", Instance(2), () => closed.Add("c2"));
        Announce(registry, "c3", "other", Instance(3), () => closed.Add("c3"));

        Assert.Equal(2, registry.Disconnect("n"));
        Assert.Equal(["c1", "c2"], closed.Order());
    }

    [Fact]
    public void ARun_TakesTheReplicaWithTheFewestRuns_ThenTheLeastRecentlyChosen_ThenTheLowestInstance()
    {
        var registry = new ReplicaRegistry();
        var one = Announce(registry, "c1", "n", Instance(1));
        var two = Announce(registry, "c2", "n", Instance(2));
        var three = Announce(registry, "c3", "n", Instance(3));

        // All idle and never chosen: the lowest instance.
        Assert.Equal(one, registry.TryReserve("r1", [("f", "n")], (_, _) => true)!["f"].Key);
        // One run each is not yet: the two others have none.
        Assert.Equal(two, registry.TryReserve("r2", [("f", "n")], (_, _) => true)!["f"].Key);
        Assert.Equal(three, registry.TryReserve("r3", [("f", "n")], (_, _) => true)!["f"].Key);
        // Everyone has one: the least recently chosen is the first again.
        Assert.Equal(one, registry.TryReserve("r4", [("f", "n")], (_, _) => true)!["f"].Key);
        registry.Release("r2");
        // Two has none now.
        Assert.Equal(two, registry.TryReserve("r5", [("f", "n")], (_, _) => true)!["f"].Key);
    }

    [Fact]
    public void AReplicaTheEligibilityRejects_IsNeverChosen_WhateverItsRuns()
    {
        var registry = new ReplicaRegistry();
        var rejected = Announce(registry, "c1", "n", Instance(1));
        var accepted = Announce(registry, "c2", "n", Instance(2));
        registry.TryReserve("busy", [("f", "n")], (r, _) => r.Key == accepted);
        registry.TryReserve("busy-2", [("f", "n")], (r, _) => r.Key == accepted);

        var chosen = registry.TryReserve("run", [("f", "n")], (r, _) => r.Key != rejected);

        Assert.Equal(accepted, chosen!["f"].Key);
    }

    [Fact]
    public void AReplicaInItsGrace_IsNeverChosen()
    {
        var registry = new ReplicaRegistry();
        Announce(registry, "c1", "n", Instance(1));
        registry.Disconnected("c1");

        Assert.Null(registry.TryReserve("run", [("f", "n")], (_, _) => true));
    }

    [Fact]
    public void AllFragmentsOfARunArePlacedTogether_OrNone()
    {
        var registry = new ReplicaRegistry();
        Announce(registry, "c1", "n", Instance(1));

        var chosen = registry.TryReserve("run", [("a", "n"), ("b", "absent-node")], (_, _) => true);

        Assert.Null(chosen);
        Assert.Empty(registry.Replicas("n").Single().Leases);
        Assert.Equal(0, registry.Replicas("n").Single().LastChosen);
    }

    [Fact]
    public void TwoFragmentsOfOneRunOnOneNode_SpreadOverItsReplicas()
    {
        var registry = new ReplicaRegistry();
        var one = Announce(registry, "c1", "n", Instance(1));
        var two = Announce(registry, "c2", "n", Instance(2));

        var chosen = registry.TryReserve("run", [("a", "n"), ("b", "n")], (_, _) => true)!;

        Assert.Equal([one, two], new[] { chosen["a"].Key, chosen["b"].Key });
    }

    [Fact]
    public void Release_GivesBackEveryLeaseOfARun_AndMayBeCalledAgain()
    {
        var registry = new ReplicaRegistry();
        Announce(registry, "c1", "n", Instance(1));
        registry.TryReserve("run", [("a", "n"), ("b", "n")], (_, _) => true);
        Assert.Equal(2, registry.Replicas("n").Single().Leases.Count);

        registry.Release("run");
        registry.Release("run");

        Assert.Empty(registry.Replicas("n").Single().Leases);
    }

    [Fact]
    public void AReadGoesToTheConnectedReplicaWithTheFewestRuns_ThenTheMostRecent()
    {
        var registry = new ReplicaRegistry();
        Announce(registry, "c1", "n", Instance(1));
        Announce(registry, "c2", "n", Instance(2));
        Announce(registry, "c3", "n", Instance(3));
        registry.TryReserve("run", [("f", "n")], (r, _) => r.ConnectionId == "c3");
        registry.Disconnected("c2");

        // c2 is in grace, c3 is busy: c1 is idle.
        Assert.Equal("c1", registry.ConnectionForRead("n"));
        registry.Release("run");
        // Idle both: the one that arrived last.
        Assert.Equal("c3", registry.ConnectionForRead("n"));
    }

    [Fact]
    public void ADataPlaneClientAnnouncedByAnotherReplica_IsHeldByOther()
    {
        var registry = new ReplicaRegistry();
        var client = Guid.NewGuid();
        var owner = Announce(registry, "c1", "n", Instance(1), client: client);
        var other = new ReplicaKey("n", Instance(2).ToString("N"));

        Assert.True(registry.DataPlaneClientHeldByOther(client, other));
        Assert.False(registry.DataPlaneClientHeldByOther(client, owner));
    }
}
