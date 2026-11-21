using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// The protection of each edge an exit carries, on the node channel's JSON protocol. A host that predates the field sends none and the
/// exit reads with none, and a reader that knows no such field still reads the exit.
/// </summary>
public class EdgeProtectionReportWireTests
{
    private static readonly JsonSerializerOptions Hub = new JsonHubProtocolOptions().PayloadSerializerOptions;

    /// <summary>What a reader that predates the field declares.</summary>
    private sealed record ReaderWithoutProtections(
        string RunId, string Fragment, int ExitCode, string Origin, string? FirstFault, IReadOnlyDictionary<string, long> RowCounts,
        string? LaunchToken = null, IReadOnlyDictionary<string, CursorReport>? Cursors = null);

    [Fact]
    public void AnExitOfAHostThatPredatesTheField_ReadsWithNoProtections()
    {
        var exit = JsonSerializer.Deserialize<FragmentExit>(
            """{"runId":"r","fragment":"p","exitCode":0,"origin":"Local","firstFault":null,"rowCounts":{"out":5},"launchToken":"t"}""", Hub)!;

        Assert.Null(exit.EdgeProtections);
        Assert.Equal(5, exit.RowCounts["out"]);
    }

    [Fact]
    public void AnExitThatCarriesTheField_IsWrittenWithIt_ReadBack_AndReadByAReaderThatKnowsNoSuchField()
    {
        var exit = new FragmentExit(
            "r", "p", 0, "Local", null, new Dictionary<string, long> { ["out"] = 5 }, "t",
            EdgeProtections: new Dictionary<string, string> { ["out"] = DataPlaneProtections.EncryptedPsk });
        var json = JsonSerializer.Serialize(exit, Hub);

        Assert.Contains("\"edgeProtections\":{\"out\":\"encrypted-psk\"}", json);
        var again = JsonSerializer.Deserialize<FragmentExit>(json, Hub)!;
        var entry = Assert.Single(again.EdgeProtections!);
        Assert.Equal(("out", "encrypted-psk"), (entry.Key, entry.Value));
        Assert.Equal("t", JsonSerializer.Deserialize<ReaderWithoutProtections>(json, Hub)!.LaunchToken);
    }
}
