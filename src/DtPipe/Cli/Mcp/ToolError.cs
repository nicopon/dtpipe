using System;
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
    public static string Describe(Exception ex)
    {
        var yaml = Find(ex);
        var reason = Simplify(Innermost(yaml ?? ex).Message);

        if (yaml is null) return ConnectionStringSanitizer.Sanitize(reason);

        var at = yaml.Start.Line > 0 ? $" (at line {yaml.Start.Line}, column {yaml.Start.Column})" : string.Empty;
        return ConnectionStringSanitizer.Sanitize($"{reason}{at}{ShapeHint(reason)}");
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
    private static string ShapeHint(string reason) =>
        reason.Contains("JobDefinition", StringComparison.Ordinal)
            ? ". The top level of a job is a map of branch aliases (e.g. 'main:'), each with its own "
              + "'input:', 'output:', 'transformers:' and 'provider-options:'. Call 'help' for the full shape."
            : string.Empty;

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
