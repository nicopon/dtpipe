using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// The category of each edge's failure an exit carries, on the node channel's JSON protocol. A host that predates the field sends none
/// and the exit reads with none, and a reader that knows no such field still reads the exit.
/// </summary>
public class EdgeFailureReportWireTests
{
    private static readonly JsonSerializerOptions Hub = new JsonHubProtocolOptions().PayloadSerializerOptions;

    /// <summary>What a reader that predates the field declares.</summary>
    private sealed record ReaderWithoutFailures(
        string RunId, string Fragment, int ExitCode, string Origin, string? FirstFault, IReadOnlyDictionary<string, long> RowCounts,
        string? LaunchToken = null, IReadOnlyDictionary<string, CursorReport>? Cursors = null,
        IReadOnlyDictionary<string, string>? EdgeProtections = null);

    [Fact]
    public void AnExitOfAHostThatPredatesTheField_ReadsWithNoFailures()
    {
        var exit = JsonSerializer.Deserialize<FragmentExit>(
            """{"runId":"r","fragment":"p","exitCode":1,"origin":"Remote","firstFault":"f","rowCounts":{"out":0},"launchToken":"t","edgeProtections":{"out":"none"}}""", Hub)!;

        Assert.Null(exit.EdgeFailures);
        Assert.Equal("none", exit.EdgeProtections!["out"]);
    }

    [Fact]
    public void AnExitThatCarriesTheField_IsWrittenWithIt_ReadBack_AndReadByAReaderThatKnowsNoSuchField()
    {
        var exit = new FragmentExit(
            "r", "p", 1, ExitOrigins.Remote, "f", new Dictionary<string, long> { ["out"] = 0 }, "t",
            EdgeFailures: new Dictionary<string, string> { ["out"] = TransferFailures.EndToEndEncryptionFailed });
        var json = JsonSerializer.Serialize(exit, Hub);

        Assert.Contains("\"edgeFailures\":{\"out\":\"EndToEndEncryptionFailed\"}", json);
        var again = JsonSerializer.Deserialize<FragmentExit>(json, Hub)!;
        var entry = Assert.Single(again.EdgeFailures!);
        Assert.Equal(("out", "EndToEndEncryptionFailed"), (entry.Key, entry.Value));
        var older = JsonSerializer.Deserialize<ReaderWithoutFailures>(json, Hub)!;
        Assert.Equal(("t", "f"), (older.LaunchToken, older.FirstFault));
    }
}
