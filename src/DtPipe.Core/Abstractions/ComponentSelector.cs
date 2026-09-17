using System;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Outcome of matching a connection string against a component's selector syntax.
/// </summary>
/// <param name="Matched">True when the string carries this component's selector.</param>
/// <param name="Cleaned">
/// The connection string with the selector removed. Providers must never see the selector:
/// a prefix-carrying string reaching an ADO.NET driver is the "dirty connection string" that
/// <see cref="IDataFactory.CanHandle"/> warns about.
/// </param>
/// <param name="Variant">
/// The optional "+{variant}" qualifier ("mysql" for "duck+mysql:"), or null when absent.
/// The selector owns the *syntax*; which variants are meaningful stays the provider's business.
/// </param>
public readonly record struct ComponentSelection(bool Matched, string Cleaned, string? Variant);

/// <summary>
/// The single authority on the <c>{component}[+{variant}]:</c> selector syntax used to route a
/// connection string to a provider.
/// <para>
/// This logic used to be reimplemented at every routing site (pipeline, inspect, MCP analyze,
/// job export, DAG rendering, provider configuration). The copies drifted: only the pipeline
/// path learned that "s3://bucket/key" is a URI rather than an "s3:"-prefixed selector, so
/// <c>inspect</c> and the MCP tools kept handing providers a mangled "//bucket/key". Centralizing
/// the grammar means a routing fix lands once instead of seven times.
/// </para>
/// </summary>
public static class ComponentSelector
{
    private static readonly ConcurrentDictionary<string, Regex> Cache = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Builds the selector pattern for one component name.
    /// <para>
    /// The trailing <c>(?!//)</c> is what keeps a remote URI from being mistaken for a selector.
    /// "s3://bucket/key.parquet" starts with "s3:" but the "//" marks it as a scheme, so it must
    /// reach the provider intact; stripping it would yield "//bucket/key.parquet". Expressing this
    /// as part of the grammar means every routing site inherits it, instead of each one needing to
    /// remember a separate URI guard.
    /// </para>
    /// </summary>
    private static Regex PatternFor(string componentName) => Cache.GetOrAdd(componentName, static name =>
        new Regex(
            @"^" + Regex.Escape(name) + @"(?:\+(?<variant>[A-Za-z0-9_.\-]+))?:(?!//)",
            RegexOptions.IgnoreCase | RegexOptions.Compiled));

    /// <summary>
    /// Matches <paramref name="raw"/> against <paramref name="componentName"/>'s selector.
    /// <list type="bullet">
    /// <item>"duck:warehouse.duckdb" → Matched, Cleaned "warehouse.duckdb", Variant null</item>
    /// <item>"duck+mysql:Host=…" → Matched, Cleaned "Host=…", Variant "mysql"</item>
    /// <item>"duck" (bare name) → Matched, Cleaned "-" (stdio), Variant null</item>
    /// <item>"s3://bucket/key" → not matched; the provider claims it through CanHandle instead</item>
    /// </list>
    /// </summary>
    public static ComponentSelection Select(string? raw, string componentName)
    {
        if (string.IsNullOrWhiteSpace(raw) || string.IsNullOrWhiteSpace(componentName))
        {
            return new ComponentSelection(false, raw ?? string.Empty, null);
        }

        var trimmed = raw.Trim();

        // A bare component name ("-o csv") selects the provider on stdio.
        if (trimmed.Equals(componentName, StringComparison.OrdinalIgnoreCase))
        {
            return new ComponentSelection(true, "-", null);
        }

        var match = PatternFor(componentName).Match(trimmed);
        if (!match.Success)
        {
            return new ComponentSelection(false, trimmed, null);
        }

        var variantGroup = match.Groups["variant"];
        return new ComponentSelection(
            true,
            trimmed[match.Length..].Trim(),
            variantGroup.Success ? variantGroup.Value : null);
    }

    /// <summary>
    /// True when <paramref name="raw"/> carries this component's selector. For sites that only
    /// need the routing verdict and not the cleaned string.
    /// </summary>
    public static bool Matches(string? raw, string componentName) => Select(raw, componentName).Matched;

    /// <summary>
    /// Any component's selector, with no name to match against. Two characters minimum, because
    /// a one-letter prefix is a Windows drive: matching it turns @"C:\data\file.csv" into
    /// @"\data\file.csv". <see cref="Select"/> is immune by construction — no component is named
    /// "C" — but a name-agnostic reader has to say so.
    /// </summary>
    private static readonly Regex AnySelectorPattern = new(
        @"^[A-Za-z0-9_.+\-]{2,}:(?!//)",
        RegexOptions.Compiled);

    /// <summary>
    /// Steps over whatever selector <paramref name="raw"/> starts with, without asking which
    /// component it names.
    /// <para>
    /// <b>Not a routing primitive.</b> It cannot tell a real selector from a string that merely
    /// looks like one, so a caller that needs to know *which* provider is addressed uses
    /// <see cref="Select"/> with a name. This exists for a reader that has to find where the
    /// connection string's own syntax begins — redaction, for one — and it lives here so the
    /// <c>(?!//)</c> that keeps "s3://bucket/key" from reading as a prefix is written once.
    /// </para>
    /// </summary>
    public static string SkipSelector(string? raw)
        => string.IsNullOrEmpty(raw) ? string.Empty : AnySelectorPattern.Replace(raw, string.Empty);
}
