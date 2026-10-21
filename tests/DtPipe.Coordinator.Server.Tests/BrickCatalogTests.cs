using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using Xunit;
using YamlDotNet.RepresentationModel;
using DtPipe.Tests.Shared;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// What the catalog says of the bricks the nodes announce, at every moment: a brick appears when its node announces it, is
/// offline while its node has no connected replica (a replica's connection dropping is no event of the inventory, only a
/// grace period running out is), and is another brick, with another version, when its owner announces it changed.
/// </summary>
public sealed class BrickCatalogTests
{
    private const string Subject = "node-x";
    private const string Source = """{"input":"sqlite:a","limit":10}""";
    private const string ChangedSource = """{"input":"sqlite:b","limit":10}""";
    private const string Sink = """{"output":"sqlite:c"}""";

    private readonly ReplicaRegistry _registry = new(new ReplicaRegistryOptions { DisconnectGracePeriod = TimeSpan.FromHours(1) });
    private readonly NodeInventory _inventory;
    private readonly BrickCatalog _catalog;
    private readonly Guid _instance = Guid.NewGuid();

    public BrickCatalogTests()
    {
        _inventory = new NodeInventory(_registry);
        _catalog = new BrickCatalog(_inventory);
    }

    private static BrickInfo Brick(string id, BrickKind kind, string branch) => TestBricks.Announced(id, kind, branch);

    /// <summary>The host announces on a connection, as the node channel does: the registry takes it, then the inventory records it.</summary>
    private void Announce(string connection, params BrickInfo[] bricks) => AnnounceAs("node-x", connection, bricks);

    private void AnnounceAs(string name, string connection, BrickInfo[] bricks)
    {
        var announcement = new NodeAnnouncement(name, "", [], NodeRole.Data, bricks, InstanceId: _instance);
        _registry.Announce(connection, Subject, announcement, ["crm"], () => { });
        _inventory.Announced(name, Subject, announcement, ["crm"]);
    }

    [Fact]
    public void A_Brick_Appears_When_Its_Node_Announces_It()
    {
        Assert.Empty(_catalog.List());

        Announce("c1", Brick("src", BrickKind.Source, Source), Brick("dst", BrickKind.Sink, Sink));

        var listed = _catalog.List();
        Assert.Equal(["node-x/dst", "node-x/src"], listed.Select(b => b.Key).Order(StringComparer.Ordinal));
        Assert.All(listed, b => Assert.True(b.Online));
        Assert.Equal("src", _catalog.Find("node-x/src")!.Id);
        var catalogued = Assert.Single(_catalog.Catalog(), c => c.Id == "src");
        Assert.Equal("node-x/src", catalogued.Key);
        Assert.Equal("""{"input":"brick:node-x/src"}""", catalogued.Branch.ToJsonString());
        Assert.Null(catalogued.Schema);
    }

    [Fact]
    public void A_Brick_Is_Offline_When_Its_Replica_Leaves_And_Gone_When_Its_Node_Is_Forgotten()
    {
        Announce("c1", Brick("src", BrickKind.Source, Source));

        _registry.Leave("c1");

        var offline = _catalog.Find("node-x/src")!;
        Assert.False(offline.Online);

        _inventory.Forget(Subject);

        Assert.Null(_catalog.Find("node-x/src"));
        Assert.Empty(_catalog.List());
    }

    /// <summary>A dropped connection puts no event through the inventory: the replica waits out its grace, and the brick is offline meanwhile.</summary>
    [Fact]
    public void A_Brick_Is_Offline_While_Its_Replica_Is_In_Grace_And_Online_Again_When_The_Host_Announces_Again()
    {
        Announce("c1", Brick("src", BrickKind.Source, Source));
        Assert.True(_catalog.Find("node-x/src")!.Online);

        _registry.Disconnected("c1");

        Assert.False(_catalog.Find("node-x/src")!.Online);
        Assert.All(_catalog.Catalog(), b => Assert.False(b.Online));

        Announce("c2", Brick("src", BrickKind.Source, Source));

        Assert.True(_catalog.Find("node-x/src")!.Online);
    }

    [Fact]
    public void A_Brick_Its_Owner_Announces_Changed_Has_Another_Version_And_The_Same_Reference()
    {
        Announce("c1", Brick("src", BrickKind.Source, Source));
        var before = _catalog.Find("node-x/src")!;

        Announce("c2", Brick("src", BrickKind.Source, ChangedSource));

        var after = _catalog.Find("node-x/src")!;
        Assert.NotEqual(before.Version, after.Version);
        Assert.Equal("brick:node-x/src", JobYaml.Scalar(_catalog.BranchOf(after), "input"));
    }

    /// <summary>A caller that edits the branch it was given changes nothing the catalog answers next.</summary>
    [Fact]
    public void A_Branch_Handed_Out_Is_A_Copy()
    {
        Announce("c1", Brick("src", BrickKind.Source, Source));
        var brick = _catalog.Find("node-x/src")!;

        var handed = _catalog.BranchOf(brick);
        JobYaml.Set(handed, "input", "sqlite:tampered");
        handed.Add("extra", "1");

        var again = _catalog.BranchOf(brick);
        Assert.Equal("brick:node-x/src", JobYaml.Scalar(again, "input"));
        Assert.False(JobYaml.Has(again, "extra"));
        Assert.Equal(brick.Version, _catalog.Find("node-x/src")!.Version);
    }

    /// <summary>The key of a brick is its node's name as it is now: the same announced bricks, filed under a new name, are found under it.</summary>
    [Fact]
    public void A_Brick_Is_Found_Under_The_Name_Its_Node_Has_Now()
    {
        BrickInfo[] bricks = [Brick("src", BrickKind.Source, Source)];
        AnnounceAs("node-x", "c1", bricks);
        Assert.NotNull(_catalog.Find("node-x/src"));

        AnnounceAs("node-y", "c2", bricks);

        Assert.Null(_catalog.Find("node-x/src"));
        var renamed = _catalog.Find("node-y/src")!;
        Assert.Equal("node-y", renamed.Node);
    }

    /// <summary>A brick announced with its text is no brick of the catalog: a node announces a version and nothing else.</summary>
    [Fact]
    public void A_Brick_Announced_With_Its_Text_Is_Never_Listed()
    {
        Announce("c1", new BrickInfo("bad", BrickKind.Source, "t", "", Source, null, null, TestBricks.VersionOf(BrickKind.Source, Source)), Brick("src", BrickKind.Source, Source));

        Assert.Equal(["node-x/src"], _catalog.List().Select(b => b.Key));
        Assert.Null(_catalog.Find("node-x/bad"));
    }
}
