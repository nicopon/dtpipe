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

    /// <summary>
    /// Collapsed is a preview and says so with an ellipsis; expanded is the whole thing. Expanding
    /// is the reader asking for all of it, and both surfaces that show it scroll.
    /// </summary>
    [Fact]
    public void Expanding_Lifts_The_Clip_Entirely()
    {
        var wall = string.Join("\n", Enumerable.Range(0, 300).Select(i => $"line {i}"));
        var step = Step(reasoning: wall);

        var collapsed = StepDetailContent.Of(step, false).Single(s => s.Kind == DetailSectionKind.Reasoning).Body;
        var expanded = StepDetailContent.Of(step, true).Single(s => s.Kind == DetailSectionKind.Reasoning).Body;

        Assert.Equal(5, collapsed.Split('\n').Length);        // 4 lines + the ellipsis line
        Assert.Equal(wall, expanded);                          // every line, no ellipsis
        Assert.DoesNotContain("…", expanded);
    }

    /// <summary>
    /// The brief form is for a surface with a frame title to put it in; the section form stays for
    /// the one without. Both read the same counts.
    /// </summary>
    [Fact]
    public void The_Brief_Usage_Names_Both_Directions_And_The_Rate()
    {
        var step = Step();
        step.Usage = new LlmUsage(PromptTokens: 3065, CompletionTokens: 117,
            GenerationTime: TimeSpan.FromSeconds(2));

        var brief = StepDetailContent.UsageBrief(step);

        Assert.Equal("3065↑ 117↓ tok · 58 tok/s", brief);
    }

    [Fact]
    public void A_Step_The_Provider_Counted_Nothing_For_Has_No_Brief_Usage()
    {
        Assert.Null(StepDetailContent.UsageBrief(Step()));
    }

    /// <summary>
    /// A model with a reasoning channel puts its intent there and leaves the content to the tool
    /// call. A recorded session shows every step of a turn arriving with an empty reasoning and a
    /// full thinking, which left the detail panel blank for the whole turn.
    /// </summary>
    [Fact]
    public void An_Empty_Reasoning_Falls_Back_To_The_Thinking_Channel()
    {
        var step = Step(reasoning: "");
        step.Thinking = "I need the schema before I can write the mapping";

        var sections = StepDetailContent.Of(step, expanded: false);
        var narration = Assert.Single(sections, s => s.Kind == DetailSectionKind.Reasoning);

        Assert.Contains("before I can write the mapping", narration.Body);
    }

    /// <summary>The same text must not be shown twice under two headings.</summary>
    [Fact]
    public void A_Borrowed_Narration_Is_Not_Repeated_As_The_Chain_Of_Thought()
    {
        var step = Step(reasoning: "");
        step.Thinking = "one and the same text";

        var kinds = StepDetailContent.Of(step, expanded: true).Select(s => s.Kind).ToArray();

        Assert.Contains(DetailSectionKind.Reasoning, kinds);
        Assert.DoesNotContain(DetailSectionKind.ChainOfThought, kinds);
    }

    /// <summary>When the model wrote both, both are shown — they are two different texts.</summary>
    [Fact]
    public void A_Model_That_Wrote_Both_Keeps_Both()
    {
        var step = Step(reasoning: "INTENT: inspect the source");
        step.Thinking = "a separate line of reasoning";

        var kinds = StepDetailContent.Of(step, expanded: true).Select(s => s.Kind).ToArray();

        Assert.Contains(DetailSectionKind.Reasoning, kinds);
        Assert.Contains(DetailSectionKind.ChainOfThought, kinds);
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
