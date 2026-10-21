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
/// grace period running out is), and is another brick, matched by another form, when its owner announces it changed.
/// </summary>
public sealed class BrickCatalogTests
{
    private const string Subject = "node-x";
    private const string Source = """{"input":"sqlite:a","limit":10}""";
    private const string ChangedSource = """{"input":"sqlite:b","limit":10}""";
    private const string Plain = """{"input":"sqlite:z"}""";
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

    private static BrickInfo Brick(string id, BrickKind kind, string branch) => TestBricks.WithText(id, kind, branch);

    /// <summary>The host announces on a connection, as the node channel does: the registry takes it, then the inventory records it.</summary>
    private void Announce(string connection, params BrickInfo[] bricks) => AnnounceAs("node-x", connection, bricks);

    private void AnnounceAs(string name, string connection, BrickInfo[] bricks)
    {
        var announcement = new NodeAnnouncement(name, "", [], NodeRole.Data, bricks, InstanceId: _instance);
        _registry.Announce(connection, Subject, announcement, ["crm"], () => { });
        _inventory.Announced(name, Subject, announcement, ["crm"]);
    }

    private static YamlMappingNode Branch(string json) => (YamlMappingNode)JobYaml.FromJson(JsonNode.Parse(json));

    [Fact]
    public void A_Brick_Appears_When_Its_Node_Announces_It()
    {
        Assert.Empty(_catalog.List());

        Announce("c1", Brick("src", BrickKind.Source, Source), Brick("dst", BrickKind.Sink, Sink));

        var listed = _catalog.List();
        Assert.Equal(["node-x/dst", "node-x/src"], listed.Select(b => b.Key).Order(StringComparer.Ordinal));
        Assert.All(listed, b => Assert.True(b.Online));
        Assert.Equal("src", _catalog.Find("node-x/src")!.Id);
        Assert.Equal("node-x/src", _catalog.Match(Branch(Source))!.Key);
        Assert.Equal("node-x/src", Assert.Single(_catalog.Catalog(), c => c.Id == "src").Key);
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
    public void A_Brick_Its_Owner_Announces_Changed_Has_Another_Version_And_Is_Matched_By_Its_New_Form_Only()
    {
        Announce("c1", Brick("src", BrickKind.Source, Source));
        var before = _catalog.Find("node-x/src")!;

        Announce("c2", Brick("src", BrickKind.Source, ChangedSource));

        var after = _catalog.Find("node-x/src")!;
        Assert.NotEqual(before.Version, after.Version);
        Assert.Null(_catalog.Match(Branch(Source)));
        Assert.Equal("node-x/src", _catalog.Match(Branch(ChangedSource))!.Key);
        Assert.Equal("sqlite:b", JobYaml.Scalar(_catalog.BranchOf(after), "input"));
    }

    /// <summary>A caller that edits the branch it was given changes nothing the catalog answers next.</summary>
    [Fact]
    public void A_Branch_Handed_Out_Is_A_Copy()
    {
        Announce("c1", Brick("src", BrickKind.Source, Source), Brick("plain", BrickKind.Source, Plain));
        var brick = _catalog.Find("node-x/src")!;

        var handed = _catalog.BranchOf(brick);
        JobYaml.Set(handed, "input", "sqlite:tampered");
        handed.Add("extra", "1");

        var again = _catalog.BranchOf(brick);
        Assert.Equal("sqlite:a", JobYaml.Scalar(again, "input"));
        Assert.False(JobYaml.Has(again, "extra"));
        Assert.Equal("node-x/src", _catalog.Match(Branch(Source))!.Key);
        Assert.Equal("node-x/plain", _catalog.Contained(Branch(Plain), BrickKind.Source)!.Key);
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

    /// <summary>
    /// The catalog keeps what it reads of an announced brick for as long as the announcement that carries it: asked twice, it
    /// hands out the very same texts, and an announcement that repeats the brick as a new object is read again.
    /// </summary>
    [Fact]
    public void What_The_Catalog_Reads_Of_A_Brick_Is_Kept_Per_Announcement()
    {
        Announce("c1", Brick("src", BrickKind.Source, Source));

        var first = _catalog.Find("node-x/src")!;
        var second = _catalog.Find("node-x/src")!;
        Assert.Same(first.Yaml, second.Yaml);
        Assert.Same(first.Version, second.Version);

        Announce("c2", Brick("src", BrickKind.Source, Source));

        var repeated = _catalog.Find("node-x/src")!;
        Assert.Equal(first.Version, repeated.Version);
        Assert.NotSame(first.Version, repeated.Version);
    }

    /// <summary>A brick whose text is no mapping is no brick of the catalog, and stays none however often it is asked.</summary>
    [Fact]
    public void A_Brick_Whose_Text_Is_No_Mapping_Is_Never_Listed()
    {
        // A node never announces a version for text that is no mapping, so the fixture carries a literal.
        Announce("c1", new BrickInfo("bad", BrickKind.Source, "t", "", "[1, 2]", null, null, "unreadable"), Brick("src", BrickKind.Source, Source));

        Assert.Equal(["node-x/src"], _catalog.List().Select(b => b.Key));
        Assert.Equal(["node-x/src"], _catalog.List().Select(b => b.Key));
        Assert.Null(_catalog.Find("node-x/bad"));
    }
}
