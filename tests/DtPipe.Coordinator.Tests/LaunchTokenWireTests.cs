using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// The node channel speaks SignalR's JSON protocol with its default options. An exit that carries no token reads as one
/// with none, which the orchestrator never admits, and a message with a field this version does not know is still read.
/// </summary>
public class LaunchTokenWireTests
{
    private static readonly JsonSerializerOptions Hub = new JsonHubProtocolOptions().PayloadSerializerOptions;

    [Fact]
    public void AnExitThatCarriesNoToken_ReadsWithNone()
    {
        var exit = JsonSerializer.Deserialize<FragmentExit>(
            """{"runId":"r","fragment":"f","exitCode":0,"origin":"Local","firstFault":null,"rowCounts":{}}""", Hub);

        Assert.Equal("r", exit!.RunId);
        Assert.Null(exit.LaunchToken);
    }

    [Fact]
    public void AnExitWithAFieldThisVersionDoesNotKnow_IsRead()
    {
        var exit = JsonSerializer.Deserialize<FragmentExit>(
            """{"runId":"r","fragment":"f","exitCode":0,"origin":"Local","firstFault":null,"rowCounts":{},"launchToken":"t","aLaterField":1}""", Hub);

        Assert.Equal("t", exit!.LaunchToken);
    }

    [Fact]
    public void ALaunchCarryingTheTokenIsWrittenWithIt_AndReadBack()
    {
        var launch = new LaunchRequest("r", new FragmentSpec("f", "v", []), "t");

        var read = JsonSerializer.Deserialize<LaunchRequest>(JsonSerializer.Serialize(launch, Hub), Hub);

        Assert.Equal("t", read!.LaunchToken);
        Assert.Contains("\"launchToken\":\"t\"", JsonSerializer.Serialize(launch, Hub));
    }
}
