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

    /// <summary>
    /// The question and its choices as display lines, the choices numbered so a reader can refer to
    /// one by number. Shared by both surfaces so they cannot number them differently.
    ///
    /// <para>
    /// A multi-line question contributes one entry per line, not one entry containing newlines: the
    /// full-screen band is a list widget that splits on them anyway, so a count that disagreed with
    /// what is on screen would make <see cref="OptionAtLine"/> pick the wrong choice.
    /// </para>
    /// </summary>
    public IReadOnlyList<string> Lines()
    {
        var lines = new List<string>(Text.Replace("\r", string.Empty).Split('\n'));
        for (int i = 0; i < Options.Count; i++)
            lines.Add($"  {i + 1}. {Options[i]}");
        return lines;
    }

    /// <summary>
    /// The terminator a model wrote into its reply instead of calling the tool, or null when the
    /// reply is an answer like any other.
    ///
    /// <para>
    /// Recognised only when the <b>whole</b> reply is that call — a bare JSON object carrying a
    /// <c>question</c>, or a <c>{"name": "ask-user", "arguments": {…}}</c> envelope, optionally
    /// inside one fenced block. A reply that merely contains such an object somewhere is an answer
    /// that quotes one, and reclassifying it would turn a delivered plan into a pending question.
    /// </para>
    ///
    /// <para>
    /// A question written as prose is deliberately NOT recognised: no rule separates it from a
    /// direct answer that happens to end in a question mark, and a wrong verdict costs more than
    /// the missed one. This reads what the model meant to emit as a call and failed to.
    /// </para>
    /// </summary>
    public static AgentQuestion? FromTextualCall(string? content)
    {
        var text = Unfence(content);
        if (text.Length == 0 || text[0] != '{') return null;

        JsonElement root;
        try
        {
            using var doc = JsonDocument.Parse(text);
            root = doc.RootElement.Clone();
        }
        catch (JsonException) { return null; }

        if (root.ValueKind != JsonValueKind.Object) return null;

        // A tool-call envelope: unwrap it to the arguments the call would have carried.
        if (root.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String
            && McpToolProvider.IsAskUser(name.GetString())
            && root.TryGetProperty("arguments", out var args) && args.ValueKind == JsonValueKind.Object)
            root = args;

        return root.TryGetProperty("question", out var q) && q.ValueKind == JsonValueKind.String
               && !string.IsNullOrWhiteSpace(q.GetString())
            ? From(root)
            : null;
    }

    /// <summary>The reply with one enclosing markdown fence removed, trimmed. A fence around
    /// anything else is left alone — this only unwraps a reply that is nothing but one block.</summary>
    private static string Unfence(string? content)
    {
        var text = (content ?? string.Empty).Trim();
        if (!text.StartsWith("```", StringComparison.Ordinal) || !text.EndsWith("```", StringComparison.Ordinal))
            return text;

        var firstBreak = text.IndexOf('\n');
        if (firstBreak < 0) return text;

        var inner = text[(firstBreak + 1)..^3].Trim();
        return inner.Contains("```", StringComparison.Ordinal) ? text : inner;
    }

    /// <summary>
    /// The choice a display line refers to, or null when that line belongs to the question itself.
    /// The choices are the trailing lines, so a question that spans several lines maps correctly —
    /// counting forward from the top would misread every option after the first newline.
    /// </summary>
    public string? OptionAtLine(int lineIndex)
    {
        if (Options.Count == 0) return null;

        var lines = Lines();
        var firstOption = lines.Count - Options.Count;
        var index = lineIndex - firstOption;
        return index >= 0 && index < Options.Count ? Options[index] : null;
    }

    public override string ToString() => string.Join('\n', Lines());
}
