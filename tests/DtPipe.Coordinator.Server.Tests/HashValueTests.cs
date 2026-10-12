using DtPipe.Coordinator.Contracts;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The hashes the server persists, shows or compares, as literal values computed outside this code. A saved plan keeps
/// its job's hash on disk, so a change to it makes every saved plan read as made from another job; a brick's version is
/// what the studio and the pages read, and a replica's bricks hash is what the Lab view shows beside it.
/// </summary>
public sealed class HashValueTests
{
    // The declaration BrickFormTests pins the canonical form of: here only its version is read, through a catalog.
    private const string Declaration = """
        {"input": "pg:Host=db;Database=shop", "provider-options": {"pg": {"query": "SELECT id,\n  name\nFROM customers", "fetch-size": 500}},
         "limit": 10}
        """;

    private readonly ReplicaRegistry _registry = new();
    private readonly NodeInventory _inventory;

    public HashValueTests() => _inventory = new NodeInventory(_registry);

    private void Announce(params BrickInfo[] bricks)
    {
        var announcement = new NodeAnnouncement("node-x", "", [], NodeRole.Data, bricks, InstanceId: Guid.NewGuid());
        _registry.Announce("connection-x", "node-x", announcement, ["crm"], () => { });
        _inventory.Announced("node-x", "node-x", announcement, ["crm"]);
    }

    [Fact]
    public void A_Pipeline_Sha_Is_The_Lowercase_Sha256_Of_Its_Text()
    {
        Assert.Equal("38d47cf1b3682cedc0c2220af95b1f31f44163315b773ddc5e1766b34ad761e5", PipelineLibrary.Sha("# P\n"));
    }

    [Fact]
    public void A_Replicas_Bricks_Hash_Is_Eight_Characters_Whatever_The_Order_They_Are_Announced_In()
    {
        Announce(
            new BrickInfo("z-src", BrickKind.Source, "t", "", """{"input":"sqlite:a"}""", null, null),
            new BrickInfo("a-sink", BrickKind.Sink, "t", "", """{"output":"sqlite:b"}""", null, null));

        var replica = Assert.Single(Assert.Single(_inventory.Snapshot()).Replicas);
        Assert.Equal("29753842", replica.Bricks);
    }

    [Fact]
    public void A_Bricks_Version_Is_Twelve_Characters_Of_The_Hash_Of_Its_Canonical_Form()
    {
        Announce(new BrickInfo("customers", BrickKind.Source, "t", "", Declaration, null, null));

        var brick = new BrickCatalog(_inventory).Find("node-x/customers");
        Assert.Equal("235ddc4e15bc", brick!.Version);
    }
}
