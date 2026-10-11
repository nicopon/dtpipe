using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// The node channel speaks SignalR's JSON protocol with its default options. A message written by a version that
/// does not know the launch token reads as one with no token, and a message with a field this version does not know is
/// still read: a coordinator and its replicas of different versions understand each other, the replica without the token
/// being kept out of runs by admission, not by a failure to read.
/// </summary>
public class LaunchTokenWireTests
{
    private static readonly JsonSerializerOptions Hub = new JsonHubProtocolOptions().PayloadSerializerOptions;

    [Fact]
    public void AnExitWrittenByAReplicaThatDoesNotKnowTheToken_ReadsWithNone()
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
    public void ALaunchWrittenByACoordinatorThatDoesNotKnowTheToken_ReadsWithNone()
    {
        var launch = JsonSerializer.Deserialize<LaunchRequest>(
            """{"runId":"r","spec":{"fragment":"f","version":"v","edges":[]}}""", Hub);

        Assert.Equal("f", launch!.Spec.Fragment);
        Assert.Null(launch.LaunchToken);
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
