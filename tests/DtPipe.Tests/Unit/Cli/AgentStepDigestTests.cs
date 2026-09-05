using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite) lot A: what a finished step leaves in scrollback is a pure function of the
/// step and the detail level. Compact keeps the trace line plus the model's stated intent; peek
/// adds a chain-of-thought preview; a final answer is just the trace line (it is rendered in full
/// elsewhere). The streaming and blocking loops both render through <see cref="StepDigest"/>.
/// </summary>
public class AgentStepDigestTests
{
    private static readonly JsonElement EmptyArgs = JsonDocument.Parse("{}").RootElement.Clone();

    private static LlmResponse Intermediate(string content, string? thinking, string tool)
    {
        var calls = new List<ToolCall> { new("c1", tool, EmptyArgs) };
        var usage = new LlmUsage(120, 340, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5));
        return new LlmResponse(new ChatMessage("assistant", content, null, calls), true, null, usage, thinking);
    }

    private const string Intent =
        "INTENT: inspect the source CSV\n" +
        "REASONING: I need the real column names before writing the pipeline\n" +
        "OBSTACLES: none so far";

    [Fact]
    public void Compact_Keeps_The_Trace_Line_And_The_Stated_Intent()
    {
        var r = Intermediate(Intent, "long private reasoning the user did not ask to see", "inspect");

        var lines = StepDigest.Lines(2, 25, TimeSpan.FromSeconds(12.3), r, "inspect", AgentDetailLevel.Compact);

        Assert.Contains("Step 2/25", lines[0]);
        Assert.Contains("inspect", lines[0]);
        Assert.Contains("340 tok", lines[0]);
        Assert.Contains("68 tok/s", lines[0]);

        Assert.Contains(lines, l => l.Contains("⏺") && l.Contains("INTENT: inspect the source CSV"));
        Assert.Contains(lines, l => l.Contains("REASONING: I need the real column names"));
        // The private chain of thought stays out of compact.
        Assert.DoesNotContain(lines, l => l.Contains("long private reasoning"));
    }

    [Fact]
    public void Peek_Adds_A_Dimmed_Chain_Of_Thought_Preview()
    {
        var r = Intermediate(Intent, "step one\nstep two\nstep three: call inspect now", "inspect");

        var lines = StepDigest.Lines(1, 25, TimeSpan.FromSeconds(4), r, null, AgentDetailLevel.Peek);

        Assert.Contains(lines, l => l.Contains("✻") && l.Contains("step three: call inspect now"));
        // Still has the intent block from compact.
        Assert.Contains(lines, l => l.Contains("INTENT: inspect the source CSV"));
    }

    [Fact]
    public void Full_Leaves_The_Reasoning_To_The_Callers_Panel_Not_An_Inline_Preview()
    {
        var r = Intermediate(Intent, "some reasoning", "inspect");

        var lines = StepDigest.Lines(1, 25, TimeSpan.FromSeconds(4), r, null, AgentDetailLevel.Full);

        Assert.DoesNotContain(lines, l => l.Contains("✻"));
        Assert.Contains(lines, l => l.Contains("INTENT: inspect the source CSV"));
    }

    [Fact]
    public void A_Final_Answer_Step_Is_Just_The_Trace_Line()
    {
        // No tool call: this step ends the turn and its content is rendered in full by
        // RenderAgentResponse — the digest must not duplicate it.
        var r = new LlmResponse(new ChatMessage("assistant", "here is the final validated plan"), true, null);

        var lines = StepDigest.Lines(3, 25, TimeSpan.FromSeconds(4), r, null, AgentDetailLevel.Full);

        Assert.Single(lines);
        Assert.Contains("Step 3/25", lines[0]);
    }

    [Fact]
    public void The_Intent_Block_Is_Bounded()
    {
        var wall = string.Join("\n", Enumerable.Range(0, 30).Select(i => $"reasoning line {i}"));
        var r = Intermediate(wall, null, "inspect");

        var lines = StepDigest.Lines(1, 25, TimeSpan.Zero, r, null, AgentDetailLevel.Compact);

        // 1 trace line + at most a handful of intent lines, never the whole wall of text.
        Assert.True(lines.Count <= 5, $"expected a bounded digest, got {lines.Count} lines");
    }

    [Fact]
    public void A_Very_Long_Reasoning_Line_Is_Truncated()
    {
        var r = Intermediate("INTENT: " + new string('x', 400), null, "inspect");

        var lines = StepDigest.Lines(1, 25, TimeSpan.Zero, r, null, AgentDetailLevel.Compact);

        var intentLine = lines.Single(l => l.Contains("⏺"));
        Assert.True(intentLine.Length < 200);
        Assert.Contains("…", intentLine);
    }

    [Fact]
    public void The_Trace_Line_Falls_Back_To_The_Streamed_Tool_Name()
    {
        // The assembled response somehow lost its tool call, but the stream saw the name.
        var r = new LlmResponse(new ChatMessage("assistant", "INTENT: do the thing"), true, null);

        var line = StepDigest.TraceLine(1, 25, TimeSpan.FromSeconds(1), r, "suggest-pipeline");

        Assert.Contains("suggest-pipeline", line);
    }
}
