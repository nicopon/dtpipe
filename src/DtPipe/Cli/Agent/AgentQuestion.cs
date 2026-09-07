using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace DtPipe.Cli.Agent;

/// <summary>
/// What the model asked with <c>ask-user</c>: the question, and the choices it offered.
///
/// <para>
/// The two travel together because <c>ask-user</c>'s schema offers both — its <c>options</c> array
/// is declared to the model as "a short list of choices, when the answer is a pick" — and carrying
/// only the text is how a model answering that schema correctly had half its answer discarded
/// before any surface could show it. A reader who cannot see the choices cannot tell a question
/// about a filename from a question about which of two columns to key on.
/// </para>
///
/// <para>
/// The choices are a suggestion, never a constraint: the reply is free text, and a caller may
/// answer something the model did not think of.
/// </para>
/// </summary>
public sealed record AgentQuestion
{
    /// <summary>Stands in when the model asked without saying what — a turn awaiting input always
    /// has something to show.</summary>
    public const string Unstated = "The agent needs more information to continue, but did not say what.";

    /// <param name="text">The question. Blank yields <see cref="Unstated"/>.</param>
    /// <param name="options">The choices offered, if any. Blank entries are dropped.</param>
    public AgentQuestion(string? text, IReadOnlyList<string>? options = null)
    {
        Text = string.IsNullOrWhiteSpace(text) ? Unstated : text.Trim();
        Options = options?.Where(o => !string.IsNullOrWhiteSpace(o)).Select(o => o.Trim()).ToList()
                  ?? (IReadOnlyList<string>)Array.Empty<string>();
    }

    public string Text { get; }

    public IReadOnlyList<string> Options { get; }

    /// <summary>Reads an <c>ask-user</c> call's arguments.</summary>
    public static AgentQuestion From(JsonElement args)
    {
        if (args.ValueKind != JsonValueKind.Object) return new AgentQuestion(text: null);

        var text = args.TryGetProperty("question", out var q) && q.ValueKind == JsonValueKind.String
            ? q.GetString()
            : null;

        var options = args.TryGetProperty("options", out var o) && o.ValueKind == JsonValueKind.Array
            ? o.EnumerateArray().Where(e => e.ValueKind == JsonValueKind.String).Select(e => e.GetString()!).ToList()
            : null;

        return new AgentQuestion(text, options);
    }

    /// <summary>The question and its choices as display lines, the choices numbered so a reader can
    /// refer to one by number. Shared by both surfaces so they cannot number them differently.</summary>
    public IReadOnlyList<string> Lines()
    {
        var lines = new List<string> { Text };
        for (int i = 0; i < Options.Count; i++)
            lines.Add($"  {i + 1}. {Options[i]}");
        return lines;
    }

    public override string ToString() => string.Join('\n', Lines());
}
