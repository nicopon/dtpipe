using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Spectre.Console;

namespace DtPipe.Cli.Agent;

/// <summary>
/// How much of each step is kept in the terminal scrollback once its live region collapses.
/// The live region itself is unaffected — this only governs the permanent trace left behind, so
/// the default stays quiet and a firehose is opt-in.
/// </summary>
public enum AgentDetailLevel
{
    /// <summary>Trace line + the model's stated intent/reasoning for an intermediate step. Default.</summary>
    Compact,

    /// <summary>Compact, plus a short dimmed preview of the tail of the model's chain of thought.</summary>
    Peek,

    /// <summary>Peek, plus the full chain of thought (rendered as a panel by the caller).</summary>
    Full,
}

/// <summary>
/// Builds the permanent record for one completed agent step. Pure and side-effect free so the exact
/// output is asserted in tests without a console. <see cref="DigestEntry"/> is the model — a
/// <see cref="TranscriptEntry"/> carrying both the plain text and the scrollback markup; the
/// streaming and blocking render paths reach it through <see cref="Lines"/>, and a full-screen
/// surface reads the same entry's <see cref="TranscriptEntry.Text"/>.
/// </summary>
internal static class StepDigest
{
    private const int IntentLineCap = 4;      // INTENT / REASONING / OBSTACLES, plus one wrapped line
    private const int PeekThinkingLineCap = 3;
    private const int LineCharCap = 110;

    /// <summary>
    /// The finished step as one transcript entry. The first markup line is always the one-line
    /// trace; the rest are the indented digest. The full chain-of-thought panel for
    /// <see cref="AgentDetailLevel.Full"/> is a renderable, not a line, so the caller adds it.
    /// </summary>
    public static TranscriptEntry DigestEntry(
        int step, int maxSteps, TimeSpan elapsed, LlmResponse response,
        string? toolFallback, AgentDetailLevel detail)
    {
        var plains = new List<string> { TracePlain(step, maxSteps, elapsed, response, toolFallback) };
        var markups = new List<string> { TraceLine(step, maxSteps, elapsed, response, toolFallback) };

        // The stated intent/reasoning. The system prompt asks the model to put INTENT / REASONING /
        // OBSTACLES in its content before every tool call — it is the cheapest answer to "what is it
        // doing and why", so it is kept at every level. A step that ends the turn with a final answer
        // is rendered in full elsewhere; showing its content here too would double it.
        bool isIntermediate = response.Message.ToolCalls is { Count: > 0 };
        if (isIntermediate)
        {
            foreach (var l in Clip(response.Message.Content, IntentLineCap))
            {
                plains.Add("⏺ " + l);
                markups.Add($"  [grey]⏺[/] {Markup.Escape(l)}");
            }
        }

        // Peek adds a short tail preview of the reasoning inline; Full leaves it to the caller's
        // panel, which carries the whole thing — an inline preview on top of it would just double.
        if (detail == AgentDetailLevel.Peek && !string.IsNullOrWhiteSpace(response.Thinking))
        {
            foreach (var l in Tail(response.Thinking!, PeekThinkingLineCap))
            {
                plains.Add("✻ " + l);
                markups.Add($"  [dim]✻ {Markup.Escape(l)}[/]");
            }
        }

        return new TranscriptEntry(TranscriptEntryKind.Digest, string.Join('\n', plains), markups);
    }

    /// <summary>Scrollback markup lines for a finished step — the markup projection of <see cref="DigestEntry"/>.</summary>
    public static IReadOnlyList<string> Lines(
        int step, int maxSteps, TimeSpan elapsed, LlmResponse response,
        string? toolFallback, AgentDetailLevel detail)
        => DigestEntry(step, maxSteps, elapsed, response, toolFallback, detail).MarkupLines;

    /// <summary>A finished tool call as one transcript entry.</summary>
    public static TranscriptEntry ToolResultEntry(string toolName, string? result, bool isError)
    {
        string snippet = result ?? "{}";
        if (snippet.Length > 200) snippet = snippet[..200] + "…";
        string text = isError ? $"↳ {toolName} [error]: {snippet}" : $"↳ {toolName}: {snippet}";
        return new TranscriptEntry(TranscriptEntryKind.ToolResult, text,
            new[] { ToolResultLine(toolName, result, isError) }, isError);
    }

    /// <summary>One line for a finished tool call — the same shape in scrollback and in the shell.</summary>
    public static string ToolResultLine(string toolName, string? result, bool isError)
    {
        string color = isError ? "red" : "green";
        string snippet = result ?? "{}";
        if (snippet.Length > 200) snippet = snippet[..200] + "…";
        string tag = isError ? " [error]" : string.Empty;
        return $"[dim]↳ {Markup.Escape(toolName)}{Markup.Escape(tag)}[/]: [bold {color}]{Markup.Escape(snippet)}[/]";
    }

    /// <summary>The model's final answer as one transcript entry.</summary>
    public static TranscriptEntry AgentResponseEntry(string? content)
    {
        content ??= string.Empty;
        var text = "🤖 dtpipe Agent\n" + string.Join('\n', content.Replace("\r", string.Empty).Split('\n'));
        return new TranscriptEntry(TranscriptEntryKind.AgentResponse, text, AgentResponseLines(content).ToList());
    }

    /// <summary>The model's final answer as markup lines — the shell has no room for a bordered panel.</summary>
    public static IEnumerable<string> AgentResponseLines(string content)
    {
        yield return "[bold green]🤖 dtpipe Agent[/]";
        foreach (var l in (content ?? string.Empty).Replace("\r", string.Empty).Split('\n'))
            yield return "  " + Markup.Escape(l);
    }

    /// <summary>A dump of the model's chain of thought as one transcript entry (detail = full).</summary>
    public static TranscriptEntry ThinkingEntry(string? thinking)
    {
        string text = thinking ?? string.Empty;
        var markup = text.Replace("\r", string.Empty).Split('\n')
            .Select(l => $"  [grey35]{Markup.Escape(l)}[/]")
            .ToList();
        return new TranscriptEntry(TranscriptEntryKind.Thinking, text, markup);
    }

    /// <summary>The one-line trace: step, wall-clock, token stats, tool, and a first-line reason.</summary>
    public static string TraceLine(int step, int maxSteps, TimeSpan elapsed, LlmResponse response, string? toolFallback)
    {
        var sb = new StringBuilder("[dim][[Step ").Append(step).Append('/').Append(maxSteps).Append("]][/]");

        sb.Append(" [grey](").Append(elapsed.TotalSeconds.ToString("F1", CultureInfo.InvariantCulture)).Append('s');
        if (response.Usage is { CompletionTokens: > 0 } u)
        {
            sb.Append(" · ").Append(u.CompletionTokens).Append(" tok");
            if (u.TokensPerSecond is { } tps)
                sb.Append(" · ").Append(tps.ToString("F0", CultureInfo.InvariantCulture)).Append(" tok/s");
            if (u.PromptEvalTime is { TotalSeconds: >= 1 } p)
                sb.Append(" · prompt ").Append(p.TotalSeconds.ToString("F0", CultureInfo.InvariantCulture)).Append('s');
        }
        sb.Append(")[/]");

        string? tool = response.Message.ToolCalls is { Count: > 0 } tc ? tc[0].Name : toolFallback;
        if (!string.IsNullOrEmpty(tool))
            sb.Append(" → [magenta]").Append(Markup.Escape(tool)).Append("[/]");

        var reason = FirstLine(response.Message.Content) ?? FirstLine(response.Thinking);
        if (!string.IsNullOrEmpty(reason))
            sb.Append(": [grey]").Append(Markup.Escape(reason)).Append("[/]");

        return sb.ToString();
    }

    /// <summary>The trace line without markup — the plain twin of <see cref="TraceLine"/> for a
    /// full-screen surface that styles the row itself.</summary>
    public static string TracePlain(int step, int maxSteps, TimeSpan elapsed, LlmResponse response, string? toolFallback)
    {
        var sb = new StringBuilder("[Step ").Append(step).Append('/').Append(maxSteps).Append(']');

        sb.Append(" (").Append(elapsed.TotalSeconds.ToString("F1", CultureInfo.InvariantCulture)).Append('s');
        if (response.Usage is { CompletionTokens: > 0 } u)
        {
            sb.Append(" · ").Append(u.CompletionTokens).Append(" tok");
            if (u.TokensPerSecond is { } tps)
                sb.Append(" · ").Append(tps.ToString("F0", CultureInfo.InvariantCulture)).Append(" tok/s");
            if (u.PromptEvalTime is { TotalSeconds: >= 1 } p)
                sb.Append(" · prompt ").Append(p.TotalSeconds.ToString("F0", CultureInfo.InvariantCulture)).Append('s');
        }
        sb.Append(')');

        string? tool = response.Message.ToolCalls is { Count: > 0 } tc ? tc[0].Name : toolFallback;
        if (!string.IsNullOrEmpty(tool))
            sb.Append(" → ").Append(tool);

        var reason = FirstLine(response.Message.Content) ?? FirstLine(response.Thinking);
        if (!string.IsNullOrEmpty(reason))
            sb.Append(": ").Append(reason);

        return sb.ToString();
    }

    /// <summary>Leading non-blank lines of <paramref name="text"/>, capped in count and width.</summary>
    private static IEnumerable<string> Clip(string? text, int maxLines)
    {
        if (string.IsNullOrWhiteSpace(text)) yield break;
        int n = 0;
        foreach (var raw in text.Replace("\r", "").Split('\n'))
        {
            var line = raw.Trim();
            if (line.Length == 0) continue;
            yield return line.Length > LineCharCap ? line[..(LineCharCap - 1)] + "…" : line;
            if (++n == maxLines) yield break;
        }
    }

    /// <summary>Trailing non-blank lines of <paramref name="text"/>, capped in count and width.</summary>
    private static IEnumerable<string> Tail(string text, int maxLines)
    {
        var kept = text.Replace("\r", "").Split('\n')
            .Select(l => l.Trim())
            .Where(l => l.Length > 0)
            .ToList();
        return kept.Skip(Math.Max(0, kept.Count - maxLines))
            .Select(l => l.Length > LineCharCap ? l[..(LineCharCap - 1)] + "…" : l);
    }

    private static string? FirstLine(string? text)
    {
        if (string.IsNullOrWhiteSpace(text)) return null;
        var line = text.Split('\n', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault()?.Trim();
        if (string.IsNullOrEmpty(line)) return null;
        return line.Length > 90 ? line[..87] + "…" : line;
    }
}
