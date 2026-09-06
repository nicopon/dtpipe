using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace DtPipe.Cli.Agent;

/// <summary>Which part of a step a <see cref="DetailSection"/> holds — the axis the two surfaces style on.</summary>
internal enum DetailSectionKind
{
    Usage,
    Reasoning,
    ChainOfThought,
    ToolName,
    ToolArgs,
    ToolOutput,
}

/// <summary>One block of a step's detail: a title, the (already clipped) body, and — for the tool
/// output — whether the call failed.</summary>
internal readonly record struct DetailSection(DetailSectionKind Kind, string Title, string Body, bool IsError = false);

/// <summary>
/// The detail of one trajectory step as UI-agnostic data. Extracted from <see cref="SessionReview"/>
/// so the full-screen detail panel and the scrollback review render the same content — the review's
/// Spectre styling stays in <see cref="SessionReview"/>, the decisions (what to include, how far to
/// clip, how to format the usage line) live here. <c>expanded</c> adds the chain of thought and the
/// tool arguments, and lifts the clips entirely: expanding is the reader asking for all of it, and
/// both surfaces that show it can scroll.
/// </summary>
internal static class StepDetailContent
{
    public static IReadOnlyList<DetailSection> Of(TrajectoryStep step, bool expanded)
    {
        var sections = new List<DetailSection>();

        if (step.Usage is { } u)
        {
            var parts = new List<string>();
            if (u.PromptTokens > 0) parts.Add($"prompt {u.PromptTokens} tok");
            if (u.CompletionTokens > 0) parts.Add($"output {u.CompletionTokens} tok");
            if (u.TokensPerSecond is { } tps) parts.Add($"{tps.ToString("F0", CultureInfo.InvariantCulture)} tok/s");
            if (parts.Count > 0)
                sections.Add(new(DetailSectionKind.Usage, "usage", string.Join("  ·  ", parts)));
        }

        if (!string.IsNullOrWhiteSpace(step.Reasoning))
            sections.Add(new(DetailSectionKind.Reasoning, "reasoning / intent",
                Clip(step.Reasoning, expanded ? 0 : 4)));

        if (expanded && !string.IsNullOrWhiteSpace(step.Thinking))
            sections.Add(new(DetailSectionKind.ChainOfThought, "chain of thought",
                Clip(step.Thinking!, 0)));

        if (!string.IsNullOrEmpty(step.ToolName))
        {
            sections.Add(new(DetailSectionKind.ToolName, "tool", step.ToolName!));

            if (expanded && !string.IsNullOrWhiteSpace(step.ToolArgs))
                sections.Add(new(DetailSectionKind.ToolArgs, "arguments", Clip(step.ToolArgs!, 0)));

            if (!string.IsNullOrWhiteSpace(step.ToolResult))
                sections.Add(new(DetailSectionKind.ToolOutput, "tool output",
                    Clip(step.ToolResult!, expanded ? 0 : 6), step.IsError));
        }

        return sections;
    }

    /// <summary>
    /// The step's token usage in one short clause, for a surface that has a frame title to put it
    /// in — arrows for direction, so it costs no words. Null when the provider counted nothing.
    /// The section form stays in <see cref="Of"/>: the scrollback review has no title to lift it
    /// into, and the two surfaces are allowed to place the same fact differently.
    /// </summary>
    internal static string? UsageBrief(TrajectoryStep step)
    {
        if (step.Usage is not { } u) return null;

        var parts = new List<string>();
        if (u.PromptTokens > 0 || u.CompletionTokens > 0)
            parts.Add($"{u.PromptTokens}↑ {u.CompletionTokens}↓ tok");
        if (u.TokensPerSecond is { } tps)
            parts.Add($"{tps.ToString("F0", CultureInfo.InvariantCulture)} tok/s");

        return parts.Count > 0 ? string.Join(" · ", parts) : null;
    }

    /// <summary>
    /// Leading <paramref name="maxLines"/> lines of <paramref name="text"/>, then an ellipsis line.
    /// Zero or less keeps everything — what a reader who expanded the step asked for.
    /// </summary>
    internal static string Clip(string text, int maxLines)
    {
        if (maxLines <= 0) return text.TrimEnd('\n');
        var lines = text.Replace("\r", string.Empty).Split('\n');
        if (lines.Length <= maxLines) return text.TrimEnd('\n');
        return string.Join('\n', lines.Take(maxLines)) + "\n…";
    }
}
