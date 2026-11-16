using System;
using System.Linq;
using System.Text.RegularExpressions;
using DtPipe.Core.Security;
using YamlDotNet.Core;

namespace DtPipe.Cli.Mcp;

/// <summary>
/// Turns an exception into something a model can act on.
///
/// <para>
/// A caller that cannot see a stack trace needs the two facts a .NET message usually buries: where
/// in its own input the failure was, and which shape was expected. A YAML failure carries both — the
/// parser records the mark it stopped at, and the reason is in the innermost exception — but the
/// text that surfaces by default is the outermost message with an assembly-qualified type name in
/// it, which reads as noise and points at nothing the caller wrote.
/// </para>
///
/// <para>
/// A line and column alone are not "where" for a caller that submitted its YAML as one escaped
/// string: counting newlines in it is the step that fails. So when the source is at hand the
/// offending line is quoted with a caret under the column. A recorded session spent four
/// iterations on <c>description: "Description for " + "category"</c> — a quoted scalar with more
/// after it — and escaped by deleting every expression in the job, delivering a plan that filled
/// a thousand rows with one constant.
/// </para>
///
/// <para>
/// This restates those facts; it does not add advice. Telling the model what a job file should look
/// like belongs to the tool descriptions it already reads, not to an error message maintained by
/// hand in a second place.
/// </para>
/// </summary>
internal static class ToolError
{
    // "DtPipe.Core.Models.JobDefinition, DtPipe.Core, Version=1.7.0.0, Culture=neutral, PublicKeyToken=null"
    private static readonly Regex AssemblyQualified = new(
        @"\b(?<type>[A-Za-z_][\w.]*)\s*,\s*[\w.]+\s*,\s*Version=[\d.]+\s*,\s*Culture=[\w-]+\s*,\s*PublicKeyToken=\w+",
        RegexOptions.Compiled);

    /// <summary>The message to hand back, sanitised and located.</summary>
    /// <param name="source">The text the caller submitted, when the tool still has it; the failing
    /// line is quoted from it.</param>
    public static string Describe(Exception ex, string? source = null)
    {
        var yaml = Find(ex);
        var reason = Simplify(Innermost(yaml ?? ex).Message);

        if (yaml is null) return ConnectionStringSanitizer.Sanitize(reason);

        var at = yaml.Start.Line > 0 ? $" (at line {yaml.Start.Line}, column {yaml.Start.Column})" : string.Empty;
        return ConnectionStringSanitizer.Sanitize($"{reason}{at}{ShapeHint(reason, KeyAt(source, yaml.Start))}{Excerpt(source, yaml.Start)}");
    }

    /// <summary>Longest line quoted whole; past it a window is taken around the column.</summary>
    private const int MaxQuoted = 160;

    /// <summary>The failing line under the message, with a caret at the column it stopped on.</summary>
    private static string Excerpt(string? source, Mark at)
    {
        if (string.IsNullOrEmpty(source) || at.Line <= 0) return string.Empty;

        var lines = source.Replace("\r\n", "\n").Split('\n');
        if (at.Line > lines.Length) return string.Empty;

        var line = lines[(int)at.Line - 1];
        var caret = (int)at.Column - 1;               // Mark.Column is 1-based
        var prefix = string.Empty;

        if (line.Length > MaxQuoted)
        {
            var from = Math.Max(0, Math.Min(caret - MaxQuoted / 2, line.Length - MaxQuoted));
            line = line.Substring(from, Math.Min(MaxQuoted, line.Length - from));
            caret -= from;
            prefix = "…";
        }

        caret = Math.Clamp(caret, 0, line.Length);
        var gutter = at.Line.ToString();

        return $"\n{gutter} | {prefix}{line}"
             + $"\n{new string(' ', gutter.Length)} | {new string(' ', prefix.Length + caret)}^";
    }

    /// <summary>
    /// A deserializer that cannot make a branch out of what it was given says so in terms of the
    /// type it wanted, which names nothing a caller can act on. The one missing fact is what the
    /// top level of a job is; the shape itself stays in 'help', which owns it.
    ///
    /// <para>
    /// A recorded session answered this error by inventing a 'job:' wrapper, which parsed as a
    /// branch of that name and validated — the wrong guess cost the rest of the turn.
    /// </para>
    /// </summary>
    private static string ShapeHint(string reason, string? key)
    {
        // A list or a map where the loader reads one string. The error names the type, never the key,
        // so the key is read off the line it stopped on: 'input:', 'output:' and 'from:' are each one
        // string, and the answer must be about the one that failed.
        if (reason.EndsWith("into type String", StringComparison.Ordinal))
            return string.Equals(key, "from", StringComparison.Ordinal)
                ? ". 'from:' is one string: the aliases of the branches this branch streams, separated by commas, "
                + "never a list. A processor streams a fixed number of 'from' branches and loads the others "
                + "from 'ref:', which is a list. Call 'help': STREAM PROCESSORS gives both numbers for each."
                : ". A branch's 'input:' and 'output:' are each one string, '<adapter-prefix>:<source>', "
                + "never a list or a map; an adapter's settings go under 'provider-options:'. "
                + "Call 'help' for the full shape.";

        // A scalar where the loader reads the options of one component: the block under
        // 'provider-options:' is a mapping, and the cast error names a CLR dictionary type, neither
        // the key nor what belongs under it.
        if (reason.StartsWith("Invalid cast from 'System.String' to 'System.Collections.Generic.Dictionary", StringComparison.Ordinal))
            return (key is null or "provider-options" ? ". Each entry of 'provider-options:'" : $". '{key}:' under 'provider-options:'")
                 + " holds the options of one component as a mapping, one option per line below it, "
                 + "never one value. 'help' gives a processor's options and 'get-adapter-help' an adapter's.";

        if (!reason.Contains("JobDefinition", StringComparison.Ordinal)) return string.Empty;

        // A key the loader could not place is a different mistake from a shape it could not read:
        // the caller got the top level right and named one thing wrong inside a branch, so the
        // answer is the list of keys a branch takes — read off the type, never written twice.
        if (reason.Contains("not found", StringComparison.Ordinal))
            return $". A branch takes: {BranchKeys}. Call 'help' for the full shape."
                 + string.Concat(ProcessSettings
                       .Where(s => reason.Contains($"Property '{s.Key}' not found", StringComparison.Ordinal))
                       .Select(s => $" {s.Where}"));

        return ". The top level of a job is a map of branch aliases (e.g. 'main:'), each with its own "
             + "'input:', 'output:', 'transformers:' and 'provider-options:'. Call 'help' for the full shape.";
    }

    /// <summary>
    /// Keys that name a setting of the process, not of a branch: refused like any unknown key, with
    /// where the setting lives, since the key alone reads as a misspelt branch key.
    /// </summary>
    private static readonly (string Key, string Where)[] ProcessSettings =
    [
        ("log-path", "A job file names no log: the log belongs to the process that runs the job, set by --log."),
    ];

    private static readonly string BranchKeys = string.Join(", ",
        typeof(DtPipe.Core.Models.JobDefinition)
            .GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
            .Select(p => Pipeline.StringExtensions.ToKebabCase(p.Name))
            .OrderBy(n => n, StringComparer.Ordinal));

    /// <summary>
    /// The key of the line a YAML failure stopped on, or null when the source is not at hand or the
    /// line holds no <c>key:</c> (a list item, a flow sequence continued on a line of its own).
    /// </summary>
    private static string? KeyAt(string? source, Mark at)
    {
        if (string.IsNullOrEmpty(source) || at.Line <= 0) return null;

        var lines = source.Replace("\r\n", "\n").Split('\n');
        if (at.Line > lines.Length) return null;

        var m = KeyOfLine.Match(lines[(int)at.Line - 1]);
        return m.Success ? m.Groups["key"].Value : null;
    }

    private static readonly Regex KeyOfLine = new(@"^\s*(?<key>[A-Za-z_][\w-]*)\s*:", RegexOptions.Compiled);

    private static YamlException? Find(Exception? ex)
    {
        for (; ex is not null; ex = ex.InnerException)
            if (ex is YamlException y) return y;
        return null;
    }

    private static Exception Innermost(Exception ex)
    {
        while (ex.InnerException is { } inner) ex = inner;
        return ex;
    }

    /// <summary>Drops the assembly, version and key from a type name, keeping the type.</summary>
    private static string Simplify(string message) =>
        AssemblyQualified.Replace(message, m =>
        {
            var type = m.Groups["type"].Value;
            int dot = type.LastIndexOf('.');
            return dot >= 0 ? type[(dot + 1)..] : type;
        });
}
