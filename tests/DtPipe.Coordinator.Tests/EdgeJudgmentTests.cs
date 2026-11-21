using DtPipe.Coordinator.Contracts;
using TransportR.Hub;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// What the replicas connected now say of each edge of a plan, by the one rule of <see cref="EdgeProtection"/>: the level an edge
/// would be held at, the sentence that refuses it, or nothing when it is not judged. The refusals of arming
/// (<see cref="ReplicaAdmission.EdgeRefusals"/>) are that judgment's sentences and no second rule.
/// </summary>
public class EdgeJudgmentTests
{
    private const string Key = "3f9a0c11d2e4b7a8";
    private const string OtherKey = "0123456789abcdef";

    private static readonly ReplicaProtection Clear = new(DataPlaneProtections.None, null);
    private static readonly ReplicaProtection Sealed = new(DataPlaneProtections.Encrypted, null);
    private static readonly ReplicaProtection PerPeer = new(DataPlaneProtections.EncryptedPsk, null);
    private static readonly ReplicaProtection Keyed = new(DataPlaneProtections.EncryptedPsk, Key);
    private static readonly ReplicaProtection OtherKeyed = new(DataPlaneProtections.EncryptedPsk, OtherKey);

    private readonly ReplicaRegistry _registry = new();
    private int _instances;

    private ReplicaAdmission Admission(string? required = null) =>
        new(_registry, new InMemoryStateStore(), required is null ? null : new AdmissionOptions { RequiredProtection = required });

    /// <summary>A replica of <paramref name="node"/> announcing <paramref name="protection"/>; its connection is named by the number it gets.</summary>
    private string Replica(string node, ReplicaProtection protection)
    {
        var instance = ++_instances;
        var connection = $"c{instance}";
        _registry.Announce(connection, node, new NodeAnnouncement(
            node, "", [], NodeRole.Runner, [], InstanceId: Guid.Parse($"00000000-0000-0000-0000-{instance:000000000000}"),
            DataPlaneProtection: protection.Level, KeyId: protection.KeyId), ["g"], () => { });
        return connection;
    }

    /// <summary>The fragments are named as the nodes they run on.</summary>
    private static RunFragment[] Fragments(params string[] nodes) =>
        [.. nodes.Select(n => new RunFragment(new FragmentSpec(n, "v1", [], Yaml: "x:\n  input: generate:1\n"), n, ["g"]))];

    private static RunEdge Edge(string producer, string consumer, string alias = "out") => new(producer, alias, consumer, "in");

    /// <summary>
    /// Arming's refusals are the sentences of the edges no pair of connected replicas joins, one per edge in the order the plan names them,
    /// whatever the aliases: an edge a node of which has no connected replica is no refusal.
    /// </summary>
    [Fact]
    public void TheRefusalsOfArming_AreTheSentencesOfTheEdgesNoPairJoins_OnePerEdgeInTheOrderOfThePlan()
    {
        Replica("a", Clear);
        Replica("b", Sealed);
        Replica("c", Keyed);
        Replica("d", OtherKeyed);
        Replica("e", Clear);
        Replica("e", Keyed);
        var offline = Replica("f", Clear);
        _registry.Disconnected(offline);

        var refusals = Admission().EdgeRefusals(
            Fragments("a", "b", "c", "d", "e", "f", "ghost"),
            [Edge("b", "c"), Edge("a", "b"), Edge("c", "d"), Edge("a", "b", "other"), Edge("d", "e"), Edge("e", "f"), Edge("f", "ghost")]);

        Assert.Equal(
            [
                "edge b -> c: b seals without a key, c seals with key 3f9a0c11d2e4b7a8.",
                "edge a -> b: a sends in clear, b seals without a key.",
                "edge c -> d: c seals with key 3f9a0c11d2e4b7a8, d seals with key 0123456789abcdef.",
                "edge d -> e: d seals with key 0123456789abcdef, e seals with key 3f9a0c11d2e4b7a8 or sends in clear.",
            ],
            refusals);
    }

    /// <summary>The edge from <c>p</c> to <c>c</c> as the replicas connected now judge it.</summary>
    private EdgeJudgment Judged(string? required = null)
    {
        var judged = Assert.Single(Admission(required).EdgeJudgments(Fragments("p", "c"), [Edge("p", "c")]));
        Assert.Equal(("p", "c"), (judged.ProducerFragment, judged.ConsumerFragment));
        return judged;
    }

    /// <summary>An edge whose ends join is held at their level, and a level of none is a level: two ends in clear are known, not unknown.</summary>
    [Theory]
    [InlineData(DataPlaneProtections.EncryptedPsk, Key, DataPlaneProtections.EncryptedPsk, Key, DataPlaneProtections.EncryptedPsk)]
    [InlineData(DataPlaneProtections.EncryptedPsk, null, DataPlaneProtections.EncryptedPsk, Key, DataPlaneProtections.EncryptedPsk)]
    [InlineData(DataPlaneProtections.EncryptedPsk, Key, DataPlaneProtections.EncryptedPsk, null, DataPlaneProtections.EncryptedPsk)]
    [InlineData(DataPlaneProtections.Encrypted, null, DataPlaneProtections.Encrypted, null, DataPlaneProtections.Encrypted)]
    [InlineData(DataPlaneProtections.None, null, DataPlaneProtections.None, null, DataPlaneProtections.None)]
    public void AnEdgeWhoseEndsJoin_IsHeldAtTheirLevel_AndAnEndWithNoKeyJoinsAnyEndOfItsLevel(
        string producerLevel, string? producerKey, string consumerLevel, string? consumerKey, string level)
    {
        Replica("p", new(producerLevel, producerKey));
        Replica("c", new(consumerLevel, consumerKey));

        var judged = Judged();

        Assert.Equal(level, judged.Protection);
        Assert.Null(judged.Refusal);
    }

    /// <summary>Ends that cannot join are refused in the sentence arming gives, which names fingerprints and never a key, and the edge holds at no level.</summary>
    [Theory]
    [InlineData(DataPlaneProtections.EncryptedPsk, Key, DataPlaneProtections.EncryptedPsk, OtherKey,
        "edge p -> c: p seals with key 3f9a0c11d2e4b7a8, c seals with key 0123456789abcdef.")]
    [InlineData(DataPlaneProtections.EncryptedPsk, Key, DataPlaneProtections.None, null,
        "edge p -> c: p seals with key 3f9a0c11d2e4b7a8, c sends in clear.")]
    [InlineData(DataPlaneProtections.None, null, DataPlaneProtections.Encrypted, null,
        "edge p -> c: p sends in clear, c seals without a key.")]
    [InlineData(DataPlaneProtections.Encrypted, null, DataPlaneProtections.EncryptedPsk, null,
        "edge p -> c: p seals without a key, c seals with a key per peer.")]
    public void AnEdgeWhoseEndsCannotJoin_IsRefusedInTheSentenceOfArming_AndHeldAtNoLevel(
        string producerLevel, string? producerKey, string consumerLevel, string? consumerKey, string sentence)
    {
        Replica("p", new(producerLevel, producerKey));
        Replica("c", new(consumerLevel, consumerKey));

        var judged = Judged();

        Assert.Null(judged.Protection);
        Assert.Equal(sentence, judged.Refusal);
        Assert.Equal([sentence], Admission().EdgeRefusals(Fragments("p", "c"), [Edge("p", "c")]));
    }

    /// <summary>
    /// An edge an end of which has no offer is not judged: no level, no refusal, never a level of none. Whether the node was never
    /// announced, has no replica connected, or has only replicas below the required protection.
    /// </summary>
    [Fact]
    public void AnEdgeAnEndOfWhichHasNoOffer_IsUnknown_NeverInClear()
    {
        Replica("c", Clear);
        Assert.Empty(Admission().EdgeJudgments(Fragments("p", "c"), []));
        Assert.Equal(new EdgeJudgment("p", "c", null, null), Judged());

        var gone = Replica("p", Clear);
        Assert.Equal(DataPlaneProtections.None, Judged().Protection);
        _registry.Disconnected(gone);
        Assert.Equal(new EdgeJudgment("p", "c", null, null), Judged());

        Replica("p", Sealed);
        Replica("c", Keyed);
        Assert.NotNull(Judged().Refusal);
        var keyed = Admission(DataPlaneProtections.EncryptedPsk);
        Assert.Equal(new EdgeJudgment("p", "c", null, null), Assert.Single(keyed.EdgeJudgments(Fragments("p", "c"), [Edge("p", "c")])));
        Assert.Empty(keyed.EdgeRefusals(Fragments("p", "c"), [Edge("p", "c")]));
    }

    /// <summary>
    /// A rolling update leaves a node with replicas that differ. The edge holds while a pair joins, at the level of the pairs that do; when
    /// the joining pairs are of several levels it is held at none that is named, and refused by no one.
    /// </summary>
    [Fact]
    public void ReplicasThatDiffer_PassWhileAPairJoins_AndTheEdgeIsHeldAtALevelOnlyWhenTheJoiningPairsShareOne()
    {
        Replica("p", Keyed);
        Replica("p", Clear);
        Replica("c", Clear);

        var one = Judged();

        Assert.Equal(DataPlaneProtections.None, one.Protection);
        Assert.Null(one.Refusal);

        Replica("c", PerPeer);

        var several = Judged();

        Assert.Null(several.Protection);
        Assert.Null(several.Refusal);
        Assert.Empty(Admission().EdgeRefusals(Fragments("p", "c"), [Edge("p", "c")]));
    }
}
