using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// The protection of the data plane an announcement carries, on the node channel's JSON protocol. A host that says nothing of it
/// reads as sealing nothing, and a reader that knows no such field still reads the announcement.
/// </summary>
public class DataPlaneProtectionWireTests
{
    private static readonly JsonSerializerOptions Hub = new JsonHubProtocolOptions().PayloadSerializerOptions;

    private const string Instance = "6f1b2a40-9c3d-4e55-8a17-0d2c3b4a5e66";

    /// <summary>What a reader that predates the two fields declares.</summary>
    private sealed record ReaderWithoutProtection(
        string Name, string Description, IReadOnlyList<DatasetInfo> Datasets, NodeRole Role, IReadOnlyList<BrickInfo> Bricks,
        IReadOnlyList<string>? Capabilities = null, string? DtpipeVersion = null, Guid? InstanceId = null, string? Origin = null,
        Guid? DataPlaneClientId = null);

    private static ReplicaView Registered(NodeAnnouncement announcement)
    {
        var registry = new ReplicaRegistry();
        registry.Announce("c1", "node-x", announcement, [], () => { });
        return Assert.Single(registry.Replicas("node-x"));
    }

    [Fact]
    public void AnAnnouncementOfAHostThatSaysNothingOfIt_ReadsAsSealingNothing_WithNoKeyId()
    {
        var announcement = JsonSerializer.Deserialize<NodeAnnouncement>(
            $$"""{"name":"n","description":"","datasets":[],"role":0,"bricks":[],"instanceId":"{{Instance}}","origin":"host:1"}""", Hub)!;

        Assert.Null(announcement.DataPlaneProtection);
        Assert.Null(announcement.KeyId);
        var view = Registered(announcement);
        Assert.Equal(DataPlaneProtections.None, view.DataPlaneProtection);
        Assert.Null(view.KeyId);
    }

    [Fact]
    public void AnAnnouncementThatCarriesIt_IsWrittenWithItsFields_ReadBack_AndReadByAReaderThatKnowsNoSuchField()
    {
        var announcement = new NodeAnnouncement(
            "n", "", [], NodeRole.Data, [], InstanceId: Guid.Parse(Instance),
            DataPlaneProtection: DataPlaneProtections.EncryptedPsk, KeyId: "0123456789abcdef");
        var json = JsonSerializer.Serialize(announcement, Hub);

        Assert.Contains("\"dataPlaneProtection\":\"encrypted-psk\"", json);
        Assert.Contains("\"keyId\":\"0123456789abcdef\"", json);
        var view = Registered(JsonSerializer.Deserialize<NodeAnnouncement>(json, Hub)!);
        Assert.Equal((DataPlaneProtections.EncryptedPsk, "0123456789abcdef"), (view.DataPlaneProtection, view.KeyId));
        Assert.Equal(Guid.Parse(Instance), JsonSerializer.Deserialize<ReaderWithoutProtection>(json, Hub)!.InstanceId);
    }
}
