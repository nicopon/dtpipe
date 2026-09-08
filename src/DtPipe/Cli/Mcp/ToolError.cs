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
        return ConnectionStringSanitizer.Sanitize($"{reason}{at}{ShapeHint(reason)}{Excerpt(source, yaml.Start)}");
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
    private static string ShapeHint(string reason)
    {
        if (!reason.Contains("JobDefinition", StringComparison.Ordinal)) return string.Empty;

        // A key the loader could not place is a different mistake from a shape it could not read:
        // the caller got the top level right and named one thing wrong inside a branch, so the
        // answer is the list of keys a branch takes — read off the type, never written twice.
        if (reason.Contains("not found", StringComparison.Ordinal))
            return $". A branch takes: {BranchKeys}. Call 'help' for the full shape.";

        return ". The top level of a job is a map of branch aliases (e.g. 'main:'), each with its own "
             + "'input:', 'output:', 'transformers:' and 'provider-options:'. Call 'help' for the full shape.";
    }

    private static readonly string BranchKeys = string.Join(", ",
        typeof(DtPipe.Core.Models.JobDefinition)
            .GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
            .Select(p => Pipeline.StringExtensions.ToKebabCase(p.Name))
            .OrderBy(n => n, StringComparer.Ordinal));

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
