using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The view a poller reads holds the step in flight and the plan's progress, touches no console and
/// keeps no journal. It is asserted here without a terminal: a reader asks when it wants the state,
/// and a push per token would follow the model's token rate.
/// </summary>
public class LiveTurnViewTests
{
    private static LlmResponse ToolStep(string content, string tool)
    {
        var calls = new List<ToolCall> { new("c1", tool, JsonDocument.Parse("{}").RootElement.Clone()) };
        return new LlmResponse(new ChatMessage("assistant", content, null, calls), true, null);
    }

    [Fact]
    public void A_View_Streams()
        => Assert.True(new LiveTurnView().CanStream);

    [Fact]
    public async Task A_Streaming_Step_Exposes_A_Live_Step_And_Clears_It_At_The_End()
    {
        var view = new LiveTurnView();
        LiveStep? during = null;

        var response = await view.StreamingStepAsync(3, 25, obs =>
        {
            obs.OnThinking("weighing the options");
            obs.OnContent("INTENT: inspect");
            obs.OnToolCall("inspect");
            during = view.LiveStepSnapshot();
            return Task.FromResult(ToolStep("INTENT: inspect", "inspect"));
        });

        Assert.NotNull(response);
        Assert.NotNull(during);
        Assert.Equal(3, during!.Value.Iteration);
        Assert.Equal("inspect", during.Value.ToolName);
        Assert.Contains("weighing the options", during.Value.Text);
        Assert.Contains("INTENT: inspect", during.Value.Text);
        // The live step is a read of what is happening, never part of the record.
        Assert.Null(view.LiveStepSnapshot());
    }

    [Fact]
    public async Task A_Streaming_Step_That_Throws_Still_Clears_The_Live_Step()
    {
        var view = new LiveTurnView();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            view.StreamingStepAsync(1, 25, obs =>
            {
                obs.OnContent("half a thought");
                throw new OperationCanceledException();
            }));

        Assert.Null(view.LiveStepSnapshot());
    }

    [Fact]
    public async Task A_Blocking_Step_Shows_A_Placeholder_And_Clears_It()
    {
        var view = new LiveTurnView();
        LiveStep? during = null;

        await view.BlockingStepAsync(4, _ =>
        {
            during = view.LiveStepSnapshot();
            return Task.FromResult(ToolStep("INTENT: go", "inspect"));
        }, CancellationToken.None);

        Assert.Equal(4, during!.Value.Iteration);
        Assert.Null(during.Value.ToolName);
        Assert.Contains("Step 4", during.Value.Text);
        Assert.Null(view.LiveStepSnapshot());
    }

    [Fact]
    public void The_Plan_Follows_The_Yaml_And_The_Results_Of_The_Tools_That_Judge_It()
    {
        var view = new LiveTurnView();
        Assert.Equal(PlanState.None, view.PlanSnapshot().State);

        view.PlanUpdated("main:\n  input: csv:a.csv\n");
        Assert.Equal(PlanState.Drafted, view.PlanSnapshot().State);

        view.ToolResult("inspect", "{}", isError: false);
        Assert.Equal(PlanState.Drafted, view.PlanSnapshot().State);

        view.ToolResult("validate-yaml-job", "{\"errors\":[\"no output\"]}", isError: true);
        var invalid = view.PlanSnapshot();
        Assert.Equal(PlanState.Invalid, invalid.State);
        Assert.Contains("no output", invalid.Message);

        view.ToolResult("validate-yaml-job", "{\"valid\":true}", isError: false);
        Assert.Equal(PlanState.Validated, view.PlanSnapshot().State);

        // A modified plan is no longer validated.
        view.PlanUpdated("main:\n  input: csv:b.csv\n");
        Assert.Equal(PlanState.Drafted, view.PlanSnapshot().State);
    }
}
