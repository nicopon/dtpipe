using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The agent transcript is UI-agnostic data. Each
/// <see cref="TranscriptEntry"/> carries a plain-text payload and the scrollback markup lines; the
/// pre-existing <c>StepDigest.Lines</c> / <c>AgentResponseLines</c> projections are now exactly
/// those markup lines, so the permanent scrollback record does not change a byte.
/// </summary>
public class TranscriptModelTests
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
        "REASONING: I need the real column names before writing the pipeline";

    [Fact]
    public void Digest_Entry_Markup_Lines_Are_Exactly_What_Lines_Projects()
    {
        var r = Intermediate(Intent, "private chain of thought", "inspect");

        foreach (var detail in new[] { AgentDetailLevel.Compact, AgentDetailLevel.Peek, AgentDetailLevel.Full })
        {
            var entry = StepDigest.DigestEntry(2, 25, TimeSpan.FromSeconds(12.3), r, "inspect", detail);
            var lines = StepDigest.Lines(2, 25, TimeSpan.FromSeconds(12.3), r, "inspect", detail);

            Assert.Equal(TranscriptEntryKind.Digest, entry.Kind);
            Assert.True(entry.MarkupLines.SequenceEqual(lines), $"projection drift at detail={detail}");
        }
    }

    [Fact]
    public void Digest_Entry_Text_Is_Plain_The_Trace_And_The_Intent_Without_Markup()
    {
        var r = Intermediate(Intent, null, "inspect");

        var entry = StepDigest.DigestEntry(2, 25, TimeSpan.FromSeconds(12.3), r, "inspect", AgentDetailLevel.Compact);

        Assert.Contains("[Step 2/25]", entry.Text);
        Assert.Contains("→ inspect", entry.Text);
        Assert.Contains("INTENT: inspect the source CSV", entry.Text);
        // No Spectre markup tags leaked into the plain payload.
        Assert.DoesNotContain("[grey]", entry.Text);
        Assert.DoesNotContain("[/]", entry.Text);
        Assert.DoesNotContain("[magenta]", entry.Text);
    }

    [Fact]
    public void A_Final_Answer_Digest_Is_A_Single_Markup_Line()
    {
        var r = new LlmResponse(new ChatMessage("assistant", "here is the final validated plan"), true, null);

        var entry = StepDigest.DigestEntry(3, 25, TimeSpan.FromSeconds(4), r, null, AgentDetailLevel.Full);

        Assert.Equal(TranscriptEntryKind.Digest, entry.Kind);
        Assert.Single(entry.MarkupLines);
        Assert.Contains("Step 3/25", entry.MarkupLines[0]);
    }

    [Fact]
    public void Tool_Result_Entry_Carries_The_Error_Flag_And_Projects_To_The_Tool_Result_Line()
    {
        var ok = StepDigest.ToolResultEntry("inspect", "{\"cols\":8}", isError: false);
        var bad = StepDigest.ToolResultEntry("execute-yaml-job", "{\"success\":false}", isError: true);

        Assert.Equal(TranscriptEntryKind.ToolResult, ok.Kind);
        Assert.False(ok.IsError);
        Assert.True(bad.IsError);

        Assert.Equal(new[] { StepDigest.ToolResultLine("inspect", "{\"cols\":8}", false) }, ok.MarkupLines);
        Assert.Equal(new[] { StepDigest.ToolResultLine("execute-yaml-job", "{\"success\":false}", true) }, bad.MarkupLines);

        // The plain payload is readable text, not markup.
        Assert.Contains("inspect", ok.Text);
        Assert.Contains("cols", ok.Text);
        Assert.DoesNotContain("[dim]", ok.Text);
    }

    [Fact]
    public void Agent_Response_Entry_Leads_With_The_Header_And_Matches_The_Line_Projection()
    {
        var entry = StepDigest.AgentResponseEntry("line one\nline two");

        Assert.Equal(TranscriptEntryKind.AgentResponse, entry.Kind);
        Assert.StartsWith("🤖 dtpipe Agent", entry.Text);
        Assert.Contains("line two", entry.Text);
        Assert.True(entry.MarkupLines.SequenceEqual(StepDigest.AgentResponseLines("line one\nline two")));
        Assert.Contains("🤖 dtpipe Agent", entry.MarkupLines[0]);
    }

    [Fact]
    public void Thinking_Entry_Keeps_The_Raw_Text_And_Dims_Every_Line()
    {
        var entry = StepDigest.ThinkingEntry("first thought\r\nsecond thought");

        Assert.Equal(TranscriptEntryKind.Thinking, entry.Kind);
        Assert.Equal("first thought\r\nsecond thought", entry.Text);
        Assert.Equal(2, entry.MarkupLines.Count);
        Assert.All(entry.MarkupLines, l => Assert.Contains("[grey35]", l));
        Assert.Contains("second thought", entry.MarkupLines[1]);
    }

    [Fact]
    public void Thinking_Entry_Tolerates_Null()
    {
        var entry = StepDigest.ThinkingEntry(null);

        Assert.Equal(string.Empty, entry.Text);
        Assert.Single(entry.MarkupLines);
    }
}
