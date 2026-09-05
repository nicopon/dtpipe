using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite) lot D3: in the persistent shell, the planning loop's output is committed to
/// the shell transcript (and, mid-stream, to the live tail) instead of the console — asserted here
/// without a terminal.
/// </summary>
public class ShellTurnViewTests
{
    private static LlmResponse ToolStep(string content, string tool)
    {
        var calls = new List<ToolCall> { new("c1", tool, System.Text.Json.JsonDocument.Parse("{}").RootElement.Clone()) };
        return new LlmResponse(new ChatMessage("assistant", content, null, calls), true, null,
            new LlmUsage(10, 42, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public void Digest_Commits_The_Step_Lines_To_The_Transcript()
    {
        var shell = new AgentShell();
        var view = new ShellTurnView(shell, () => { });

        view.Digest(1, 25, TimeSpan.FromSeconds(3), ToolStep("INTENT: inspect the source", "inspect"), AgentDetailLevel.Compact);

        Assert.Contains(shell.Transcript, l => l.Contains("Step 1/25"));
        Assert.Contains(shell.Transcript, l => l.Contains("INTENT: inspect the source"));
    }

    [Fact]
    public void ToolResult_And_AgentResponse_Become_Transcript_Lines()
    {
        var shell = new AgentShell();
        var view = new ShellTurnView(shell, () => { });

        view.ToolResult("inspect", "{\"cols\":8}", isError: false);
        view.AgentResponse("here is the validated plan");

        Assert.Contains(shell.Transcript, l => l.Contains("inspect") && l.Contains("cols"));
        Assert.Contains(shell.Transcript, l => l.Contains("🤖"));
        Assert.Contains(shell.Transcript, l => l.Contains("here is the validated plan"));
    }

    [Fact]
    public async Task Streaming_Step_Feeds_The_Live_Tail_Then_Commits_The_Digest()
    {
        var shell = new AgentShell();
        var view = new ShellTurnView(shell, () => { });
        string? tailDuringStream = null;

        var result = await view.StreamingStepAsync(2, 25, AgentDetailLevel.Compact, obs =>
        {
            obs.OnThinking("weighing options");
            obs.OnContent("INTENT: validate");
            tailDuringStream = shell.LiveTail;   // the tail is live while the model streams
            return Task.FromResult(ToolStep("INTENT: validate", "validate-yaml-job"));
        });

        Assert.NotNull(tailDuringStream);
        Assert.Contains("INTENT: validate", tailDuringStream);
        Assert.Null(shell.LiveTail);              // cleared once the stream ends
        Assert.Contains(shell.Transcript, l => l.Contains("Step 2/25"));
        Assert.Equal("validate-yaml-job", result.Message.ToolCalls![0].Name);
    }

    [Fact]
    public async Task Blocking_Step_Shows_A_Placeholder_Tail_And_Clears_It()
    {
        var shell = new AgentShell();
        var view = new ShellTurnView(shell, () => { });
        string? tailDuringCall = null;

        await view.BlockingStepAsync(4, ct =>
        {
            tailDuringCall = shell.LiveTail;
            return Task.FromResult(ToolStep("thinking", "inspect"));
        }, CancellationToken.None);

        Assert.Contains("Step 4", tailDuringCall);
        Assert.Null(shell.LiveTail);
    }

    [Fact]
    public async Task The_Live_Tail_Is_Cleared_Even_If_The_Call_Throws()
    {
        var shell = new AgentShell();
        var view = new ShellTurnView(shell, () => { });

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            view.StreamingStepAsync(1, 25, AgentDetailLevel.Compact,
                _ => throw new OperationCanceledException()));

        Assert.Null(shell.LiveTail);
    }

    [Fact]
    public void Full_Detail_Appends_The_Whole_Chain_Of_Thought()
    {
        var shell = new AgentShell();
        var view = new ShellTurnView(shell, () => { });
        var step = new LlmResponse(
            new ChatMessage("assistant", "INTENT: x", null,
                new List<ToolCall> { new("c1", "inspect", System.Text.Json.JsonDocument.Parse("{}").RootElement.Clone()) }),
            true, null, null, "line one of reasoning\nline two of reasoning");

        view.Digest(1, 25, TimeSpan.Zero, step, AgentDetailLevel.Full);

        Assert.Contains(shell.Transcript, l => l.Contains("line two of reasoning"));
    }

    [Fact]
    public void Every_Emission_Repaints()
    {
        int repaints = 0;
        var v = new ShellTurnView(new AgentShell(), () => repaints++);

        v.ToolResult("inspect", "{}", false);
        v.Digest(1, 25, TimeSpan.Zero, ToolStep("x", "inspect"), AgentDetailLevel.Compact);
        v.AgentResponse("done");

        Assert.True(repaints >= 3);
    }
}
