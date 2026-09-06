using System;
using System.Collections.Generic;

namespace DtPipe.Cli.Agent.Tui;

/// <summary>What the agent's last word was, which decides the marker the exchange wears.</summary>
internal enum ExchangeKind
{
    /// <summary>A turn is running and the model is producing text.</summary>
    Speaking,

    /// <summary>The agent stopped to ask something. The next line typed is the answer.</summary>
    Question,

    /// <summary>The turn delivered.</summary>
    Done,

    /// <summary>The turn ended without a result.</summary>
    Stopped,

    /// <summary>The session itself answering — a command's outcome, not the agent's.</summary>
    Note,
}

/// <summary>
/// One exchange as the surface shows it: a marker, a headline that names the expected contribution,
/// and the body. Pure, so the whole vocabulary is asserted without a terminal.
///
/// <para>
/// The markers stay inside the Basic Multilingual Plane. <see cref="TurnSummaryModel.StatusWord"/>
/// spells the same three states with emoji, but that string is the scrollback table's and is
/// rendered into a stream where width does not matter; here every glyph shares a line with text in
/// a fixed grid, and an emoji is drawn two cells wide.
/// </para>
/// </summary>
/// <param name="Kind">What kind of word this is.</param>
/// <param name="Headline">The state and what the user is expected to do about it.</param>
/// <param name="Body">The agent's words, or the note's detail. May be empty.</param>
internal readonly record struct Exchange(ExchangeKind Kind, string Headline, string Body)
{
    /// <summary>The one-cell glyph in the gutter.</summary>
    public string Marker => Kind switch
    {
        ExchangeKind.Speaking => "▸",
        ExchangeKind.Question => "?",
        ExchangeKind.Done => "✓",
        ExchangeKind.Stopped => "✗",
        _ => "•",
    };

    /// <summary>The body split into display lines, empty when there is nothing to say.</summary>
    public IReadOnlyList<string> BodyLines() => string.IsNullOrWhiteSpace(Body)
        ? Array.Empty<string>()
        : Body.Replace("\r", string.Empty).Split('\n');
}

/// <summary>
/// Builds the exchange from whatever the session last did. One factory, so the marker vocabulary and
/// the wording of "what happens next" have a single author rather than being spelled at each of the
/// nine places that post to the band.
/// </summary>
internal static class ExchangeContent
{
    /// <summary>The model is mid-turn; <paramref name="tail"/> is what it has said so far.</summary>
    public static Exchange Speaking(string? tail)
        => new(ExchangeKind.Speaking, "working", (tail ?? string.Empty).Trim());

    /// <summary>The session speaking for itself — a command's outcome.</summary>
    public static Exchange Note(string line)
        => new(ExchangeKind.Note, line.Trim(), string.Empty);

    /// <summary>
    /// A finished turn. The headline says what to do next, not only what happened: a question wants
    /// an answer, a delivered plan wants running or amending, a stopped turn wants rewording.
    /// </summary>
    public static Exchange Of(TurnSummaryModel summary, bool hasPlan)
        => summary.Status switch
        {
            TurnStatus.AwaitingInput => new Exchange(
                ExchangeKind.Question,
                "QUESTION — answer below",
                (summary.Question ?? string.Empty).Trim()),

            TurnStatus.Completed => new Exchange(
                ExchangeKind.Done,
                hasPlan ? "DONE — /exec to run it, or ask for a change" : "DONE — ask for anything else",
                Tally(summary)),

            _ => new Exchange(
                ExchangeKind.Stopped,
                $"STOPPED — {summary.Reason}",
                Tally(summary)),
        };

    private static string Tally(TurnSummaryModel s)
    {
        var line = $"{s.Iterations} step{(s.Iterations == 1 ? "" : "s")} · {s.DurationText}";
        return s.ToolSummary is { } tools ? $"{line} · {tools}" : line;
    }
}
