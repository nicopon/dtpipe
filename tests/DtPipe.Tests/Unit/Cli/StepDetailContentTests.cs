using System;
using System.Linq;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A step's detail as UI-agnostic sections, shared by the full-screen
/// panel and the scrollback review. Expanding widens the clips and reveals the chain of thought
/// and the tool arguments. <see cref="AgentSessionReviewTests"/> proves the review still renders
/// the same thing off these sections.
/// </summary>
public class StepDetailContentTests
{
    private static TrajectoryStep Step(
        string reasoning = "INTENT: inspect the source",
        string? thinking = null,
        string? tool = null,
        string? args = null,
        string? result = null,
        bool isError = false,
        LlmUsage? usage = null)
        => new()
        {
            Iteration = 1,
            Reasoning = reasoning,
            Thinking = thinking,
            ToolName = tool,
            ToolArgs = args,
            ToolResult = result,
            IsError = isError,
            Usage = usage,
        };

    [Fact]
    public void Collapsed_Has_The_Usage_The_Reasoning_And_The_Tool_Output()
    {
        var step = Step(tool: "inspect", result: "8 columns", usage: new LlmUsage(120, 340, null, TimeSpan.FromSeconds(5)));

        var kinds = StepDetailContent.Of(step, expanded: false).Select(s => s.Kind).ToArray();

        Assert.Equal(new[]
        {
            DetailSectionKind.Usage,
            DetailSectionKind.Reasoning,
            DetailSectionKind.ToolName,
            DetailSectionKind.ToolOutput,
        }, kinds);
    }

    [Fact]
    public void The_Usage_Line_Reads_Like_The_Review()
    {
        var step = Step(usage: new LlmUsage(120, 340, null, TimeSpan.FromSeconds(5)));

        var usage = StepDetailContent.Of(step, false).Single(s => s.Kind == DetailSectionKind.Usage);

        Assert.Equal("prompt 120 tok  ·  output 340 tok  ·  68 tok/s", usage.Body);
    }

    [Fact]
    public void Expanding_Adds_The_Chain_Of_Thought_And_The_Arguments()
    {
        var step = Step(thinking: "weighing the options", tool: "inspect", args: "{\"input\":\"csv:x\"}", result: "ok");

        var collapsed = StepDetailContent.Of(step, false).Select(s => s.Kind).ToArray();
        var expanded = StepDetailContent.Of(step, true).Select(s => s.Kind).ToArray();

        Assert.DoesNotContain(DetailSectionKind.ChainOfThought, collapsed);
        Assert.DoesNotContain(DetailSectionKind.ToolArgs, collapsed);
        Assert.Contains(DetailSectionKind.ChainOfThought, expanded);
        Assert.Contains(DetailSectionKind.ToolArgs, expanded);
    }

    [Fact]
    public void Expanding_Widens_The_Reasoning_Clip()
    {
        var wall = string.Join("\n", Enumerable.Range(0, 30).Select(i => $"line {i}"));
        var step = Step(reasoning: wall);

        var collapsed = StepDetailContent.Of(step, false).Single(s => s.Kind == DetailSectionKind.Reasoning).Body;
        var expanded = StepDetailContent.Of(step, true).Single(s => s.Kind == DetailSectionKind.Reasoning).Body;

        Assert.Equal(5, collapsed.Split('\n').Length);        // 4 lines + the ellipsis line
        Assert.True(expanded.Split('\n').Length > collapsed.Split('\n').Length);
    }

    [Fact]
    public void The_Tool_Output_Section_Carries_The_Error_Flag()
    {
        var ok = StepDetailContent.Of(Step(tool: "x", result: "done", isError: false), false);
        var bad = StepDetailContent.Of(Step(tool: "x", result: "boom", isError: true), false);

        Assert.False(ok.Single(s => s.Kind == DetailSectionKind.ToolOutput).IsError);
        Assert.True(bad.Single(s => s.Kind == DetailSectionKind.ToolOutput).IsError);
    }

    [Fact]
    public void A_Reasoning_Only_Step_Has_Just_The_Reasoning()
    {
        var kinds = StepDetailContent.Of(Step(), false).Select(s => s.Kind).ToArray();
        Assert.Equal(new[] { DetailSectionKind.Reasoning }, kinds);
    }

    [Fact]
    public void An_Empty_Step_Yields_No_Sections()
    {
        Assert.Empty(StepDetailContent.Of(Step(reasoning: ""), false));
    }
}
