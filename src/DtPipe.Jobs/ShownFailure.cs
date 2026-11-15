using System.Text.RegularExpressions;
using YamlDotNet.Core;

namespace DtPipe.Jobs;

/// <summary>
/// The one wording of a failure shown to a person: the cause that was thrown first, its first line, and where the job's
/// text stopped parsing when it did. Never the type of an exception, a trace, a reader's dump of its state or a link of the
/// chain that only says what was being done. The wording is a shape of the message and names no provider: an engine's
/// failure arrives either as an exception or as the flattened chain <c>-&gt; Type: message</c> a branch reports, and both
/// reduce to the same line.
/// </summary>
public static partial class ShownFailure
{
    /// <summary>The cause of <paramref name="failure"/>: its innermost exception's first line, with the line of a YAML failure.</summary>
    public static string Of(Exception failure)
    {
        var text = Of(failure.GetBaseException().Message);
        for (var e = failure; e is not null; e = e.InnerException)
        {
            if (e is YamlException { Start.Line: > 0 } yaml)
                return $"{text} (at line {yaml.Start.Line}, column {yaml.Start.Column})";
        }
        return text;
    }

    /// <summary>
    /// The cause a message states: the last link of a flattened chain without its type, otherwise the first line. A message
    /// that is empty gives an empty string, and the caller says what it knows instead.
    /// </summary>
    public static string Of(string? message)
    {
        if (string.IsNullOrWhiteSpace(message)) return "";
        var lines = message.Replace("\r\n", "\n").Split('\n').Select(l => l.Trim()).Where(l => l.Length > 0).ToList();
        var last = lines.FindLastIndex(l => l.StartsWith("-> ", StringComparison.Ordinal));
        if (last < 0) return lines[0];
        var link = lines[last][3..];
        var typed = ChainType().Match(link);
        return typed.Success ? link[typed.Length..] : link;
    }

    /// <summary>
    /// The cause in what a dtpipe child wrote on its console: the last link of the chain it reports, whole, without its type,
    /// its log lines, its verdict lines, a wrapped line's break or the flag the check runs under, which a person reads as a
    /// published job. A line that ends in a space continues on the next, as the console wraps one; text that has lost those
    /// spaces keeps its lines.
    /// </summary>
    public static string OfConsole(string? console)
    {
        if (string.IsNullOrWhiteSpace(console)) return "";
        var joined = new List<string>();
        foreach (var line in console.Replace("\r\n", "\n").Split('\n'))
        {
            if (joined.Count > 0 && joined[^1].EndsWith(' ') && line.Trim().Length > 0 && !IsNoise(line)) joined[^1] += line;
            else joined.Add(line);
        }
        var lines = joined.Select(l => l.Trim()).Where(l => l.Length > 0 && !IsNoise(l)).ToList();
        if (lines.Count == 0) return "";
        var last = lines.FindLastIndex(l => l.StartsWith("-> ", StringComparison.Ordinal));
        if (last >= 0)
        {
            lines = lines[last..];
            lines[0] = Of(lines[0]);
        }
        else if (lines[0].StartsWith("Error: ", StringComparison.Ordinal)) lines[0] = lines[0]["Error: ".Length..];
        return string.Join('\n', lines)
            .Replace("under --edges-only", "in a published job", StringComparison.Ordinal)
            .Replace("--edges-only", "a published job", StringComparison.Ordinal);
    }

    // A log line ("[23:08:05 ERR] ..."), a verdict mark, the panel's frame and the caret under a query's column.
    private static bool IsNoise(string line)
    {
        var text = line.TrimStart();
        return LogLine().IsMatch(text) || text.StartsWith('✖') || text.StartsWith('✔') || text.StartsWith('╭') || text.StartsWith('│')
            || text.StartsWith('╰') || text == "^";
    }

    // "TypeName: " at the start of a chain link: one token, so "Binder Error: ..." is left whole.
    [GeneratedRegex(@"^[\w.`\[\]]+: ")]
    private static partial Regex ChainType();

    [GeneratedRegex(@"^\[\d{2}:\d{2}:\d{2} [A-Z]{3}\]")]
    private static partial Regex LogLine();
}
