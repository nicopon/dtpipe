using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// Eligibility is a fragment's, joining is an edge's: a run is placed only where the two ends of every edge protect the data plane
/// alike, its replicas chosen together, and a run no choice joins is refused before anything is launched, naming the edge and what
/// each end does.
/// </summary>
public class EdgeProtectionTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);
    private const string Key = "3f9a0c11d2e4b7a8";
    private const string OtherKey = "0123456789abcdef";

    private static readonly ReplicaProtection Clear = new(DataPlaneProtections.None, null);
    private static readonly ReplicaProtection Sealed = new(DataPlaneProtections.Encrypted, null);
    private static readonly ReplicaProtection PerPeer = new(DataPlaneProtections.EncryptedPsk, null);
    private static readonly ReplicaProtection Keyed = new(DataPlaneProtections.EncryptedPsk, Key);
    private static readonly ReplicaProtection OtherKeyed = new(DataPlaneProtections.EncryptedPsk, OtherKey);

    private static Guid Instance(int n) => Guid.Parse($"00000000-0000-0000-0000-{n:000000000000}");

    private static ReplicaKey Announce(ReplicaRegistry registry, string node, int instance, ReplicaProtection protection) =>
        registry.Announce("c" + instance, node, new NodeAnnouncement(
            node, "", [], NodeRole.Runner, [], InstanceId: Instance(instance),
            DataPlaneProtection: protection.Level, KeyId: protection.KeyId), ["g"], () => { });

    [Fact]
    public void TwoEndsJoin_OnOneLevel_WithOneKeyIdOrNone_NeverTwoKeyIdsOrTwoLevels()
    {
        Assert.True(Keyed.Joins(Keyed));
        Assert.True(Keyed.Joins(PerPeer));
        Assert.True(PerPeer.Joins(OtherKeyed));
        Assert.False(Keyed.Joins(OtherKeyed));
        Assert.False(Clear.Joins(Sealed));
        Assert.False(Sealed.Joins(PerPeer));
        Assert.False(Keyed.Joins(Clear));
    }

    /// <summary>
    /// The replica the order of preference puts first on <c>n</c> holds a key the other end does not, the second names none: a key
    /// per peer joins any key, so the second is taken, and the choice for <c>a</c> is made with the edge to <c>b</c> in view.
    /// </summary>
    [Fact]
    public void AReplicaThatJoinsTheOtherEnd_IsTaken_WhereTheOrderOfPreferenceWouldTakeOneThatDoesNot()
    {
        var registry = new ReplicaRegistry();
        Announce(registry, "n", 1, Keyed);
        var joining = Announce(registry, "n", 2, PerPeer);
        var other = Announce(registry, "m", 3, OtherKeyed);

        var chosen = registry.TryReserve("run", [("a", "n"), ("b", "m")], (_, _) => true, [("a", "b")]);

        Assert.Equal((joining, other), (chosen!["a"].Key, chosen["b"].Key));
    }

    /// <summary>Every replica sends in clear: the edges change nothing, and the run is placed by the order of preference alone.</summary>
    [Fact]
    public void ARunWhoseReplicasAllProtectAlike_IsPlacedAsTheOrderOfPreferenceAlonePlacesIt()
    {
        var registry = new ReplicaRegistry();
        var one = Announce(registry, "n", 1, Clear);
        var two = Announce(registry, "n", 2, Clear);
        var three = Announce(registry, "m", 3, Clear);
        registry.TryReserve("busy", [("f", "n")], (_, _) => true);

        var chosen = registry.TryReserve("run", [("a", "n"), ("b", "n"), ("c", "m")], (_, _) => true, [("a", "b"), ("b", "c")]);

        Assert.Equal([two, one, three], new[] { chosen!["a"].Key, chosen["b"].Key, chosen["c"].Key });
    }

    private static Task<CoordinatorTestHost> StartAsync(TestJwt jwt) =>
        CoordinatorTestHost.StartAsync(_ => { }, services => services.AddSingleton(new RunOrchestratorOptions
        {
            AdmissionTimeout = TimeSpan.FromSeconds(1),
            ReadyTimeout = TimeSpan.FromSeconds(2),
            ExitTimeout = TimeSpan.FromSeconds(1),
        }), jwt);

    private static Task<FakeNode> NodeAsync(CoordinatorTestHost host, TestJwt jwt, string node, ReplicaProtection protection) =>
        FakeNode.StartAsync(host, node, new FakeNodeOptions
        {
            Token = jwt.Mint(node, "test", TimeSpan.FromHours(1)), DataPlaneProtection = protection.Level, KeyId = protection.KeyId,
        });

    /// <summary>The chain the fragments name, each fragment on the node of the same name, one edge between each two that follow.</summary>
    private static RunSpec Chain(string runId, params string[] nodes)
    {
        RunFragment FragmentAt(int i)
        {
            var edges = new List<FragmentEdge>();
            if (i > 0) edges.Add(new FragmentEdge("in", FragmentEdgeDirection.Inbound));
            if (i < nodes.Length - 1) edges.Add(new FragmentEdge("out", FragmentEdgeDirection.Outbound));
            return new RunFragment(new FragmentSpec(nodes[i], "v1", edges, Yaml: "x:\n  input: generate:1\n"), nodes[i], ["test"]);
        }
        return new RunSpec(
            runId, [.. Enumerable.Range(0, nodes.Length).Select(FragmentAt)],
            [.. nodes.Zip(nodes.Skip(1), (producer, consumer) => new RunEdge(producer, "out", consumer, "in"))]);
    }

    [Fact]
    public async Task ARunNoChoiceOfReplicasJoins_IsRefusedNamingTheEdgeAndWhatEachEndDoes_AndLeavesNoLease()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        await using var crm = await NodeAsync(host, jwt, "crm", Keyed);
        await using var runner = await NodeAsync(host, jwt, "runner-1", Clear);
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var ex = await Assert.ThrowsAsync<NoEligibleReplicaException>(
            () => orchestrator.RunAsync(Chain("run-1", "crm", "runner-1")).WaitAsync(Wait));

        var (edge, ends) = Assert.Single(ex.Reasons);
        Assert.Equal("edge crm -> runner-1", edge);
        Assert.Equal($"crm seals with key {Key}, runner-1 sends in clear", ends);
        Assert.Empty(crm.Launches);
        Assert.Empty(runner.Launches);
        Assert.All(host.Host.Services.GetRequiredService<ReplicaRegistry>().Replicas(), r => Assert.Empty(r.Leases));
    }

    /// <summary>
    /// A chain A -> B -> C where B seals, C sends in clear and A does either: a run of it would fail before its first batch, so
    /// admission refuses it before any fragment is launched, leasing nothing, and names each edge that cannot join.
    /// </summary>
    [Theory]
    [InlineData(false, "edge a -> b|a sends in clear, b seals without a key", "edge b -> c|b seals without a key, c sends in clear")]
    [InlineData(true, "edge b -> c|b seals without a key, c sends in clear")]
    public async Task AChainWhoseEndsProtectTheDataPlaneDifferently_IsRefusedBeforeAnyFragmentIsLaunched(bool aSeals, params string[] refused)
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);
        await using var a = await NodeAsync(host, jwt, "a", aSeals ? Sealed : Clear);
        await using var b = await NodeAsync(host, jwt, "b", Sealed);
        await using var c = await NodeAsync(host, jwt, "c", Clear);
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var ex = await Assert.ThrowsAsync<NoEligibleReplicaException>(
            () => orchestrator.RunAsync(Chain("run-1", "a", "b", "c")).WaitAsync(Wait));

        Assert.Equal(refused, ex.Reasons.Select(r => $"{r.Key}|{r.Value}"));
        Assert.Empty(a.Launches.Concat(b.Launches).Concat(c.Launches));
        Assert.All(host.Host.Services.GetRequiredService<ReplicaRegistry>().Replicas(), r => Assert.Empty(r.Leases));
    }
}
