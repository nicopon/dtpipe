using DtPipe.Adapters.Common;
using DtPipe.Cli.Security;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;

namespace DtPipe.Sessions;

/// <summary>How far a sample run could actually guarantee it would not write.</summary>
public enum ReadOnlyEnforcement
{
    /// <summary>The database was put in a read-only session: it refuses a write itself.</summary>
    ServerEnforced,
    /// <summary>No server-side equivalent; only the conservative verb scan ran.</summary>
    VerbScanOnly
}

public sealed record SampleSafetyVerdict(
    bool Allowed,
    ReadOnlyEnforcement Enforcement,
    IReadOnlyList<string> Violations);

/// <summary>
/// Refuses a sample run whose SOURCE could mutate.
///
/// "Writes cut off" is a property of the WRITER, not of the run. Neutralising the writer does
/// nothing about a reader that mutates on its way past — <c>DELETE … RETURNING</c> on
/// PostgreSQL, <c>… OUTPUT</c> on SQL Server, arbitrary SQL in --duck-init, an ATTACH inside
/// --sql. And <c>--limit 10</c> bounds what the client reads, never what the server already
/// destroyed.
///
/// Two differences from the guardrail that existed before:
///
/// <list type="bullet">
/// <item><b>It classifies the RESOLVED pipeline, not the YAML text.</b> The MCP pre-check runs
/// before IStringContentResolver, so <c>query: "@/tmp/x.sql"</c> passed it with its contents
/// never seen.</item>
/// <item><b>It covers the CLI too.</b> There was exactly one call site for ISqlSafetyPolicy in
/// the repository, on execute-yaml-job; the MCP dry-run tool and --dry-run had none — they were
/// protected by not executing, and this path executes.</item>
/// </list>
///
/// It reuses <see cref="ISqlSafetyPolicy"/> rather than growing a second policy: two
/// classifications of "destructive" that could disagree is the duplication this cycle removes,
/// transposed onto safety.
/// </summary>
public static class SampleModeSafetyGate
{
    /// <summary>
    /// Classifies everything SQL-bearing that a sample run would execute.
    ///
    /// A comment is not executed, so each value is classified without its comments, by the rule of
    /// <see cref="SqlQueryGuard.StripComments"/>; a text that rule cannot read the way every engine
    /// does is classified whole. That also stops a network pattern inside a comment from refusing the
    /// run. The policy itself still scans raw text: the MCP pre-check hands it YAML, which is not SQL.
    /// </summary>
    public static SampleSafetyVerdict Evaluate(
        ISqlSafetyPolicy policy,
        SqlSafetyOptions options,
        IEnumerable<string?> sqlBearingValues,
        ISqlDialect? dialect)
    {
        var violations = new List<string>();

        foreach (var value in sqlBearingValues)
        {
            if (string.IsNullOrWhiteSpace(value)) continue;
            var result = policy.Analyze(SqlQueryGuard.StripComments(value), options);
            if (!result.Allowed) violations.AddRange(result.Violations);
        }

        var enforcement = dialect?.ReadOnlySessionSql is not null
            ? ReadOnlyEnforcement.ServerEnforced
            : ReadOnlyEnforcement.VerbScanOnly;

        return new SampleSafetyVerdict(violations.Count == 0, enforcement, violations.Distinct().ToList());
    }

    /// <summary>
    /// Collects the SQL a sample run would actually execute against the SOURCE: the reader's
    /// query and any provider option carrying SQL, such as an init script.
    ///
    /// Writer hooks are deliberately NOT classified. All four are suppressed in sample mode
    /// (see PrepareSampleWriterAsync), so refusing a pipeline for carrying one would add no
    /// safety and would refuse previews of perfectly ordinary jobs — a pipeline whose real run
    /// truncates its target is exactly the kind a user wants to preview. An over-broad guard
    /// teaches people to pass --allow-destructive by reflex, which would then unlock the source
    /// side too; that is worse than no guard.
    /// </summary>
    public static IEnumerable<string?> CollectSqlBearingValues(OptionsRegistry registry, Type? readerOptionsType)
    {
        if (readerOptionsType is null) yield break;

        var readerOpts = registry.Get(readerOptionsType);
        if (readerOpts is IQueryAwareOptions q) yield return q.Query;
        foreach (var v in SqlishProperties(readerOpts)) yield return v;
    }

    /// <summary>
    /// String options whose name marks them as carrying SQL — init scripts, queries and the
    /// like. Reflection rather than a hard-coded list: a provider that adds one gets covered
    /// without this file being edited, which is the failure mode a list would have.
    /// </summary>
    private static IEnumerable<string?> SqlishProperties(object? options)
    {
        if (options is null) yield break;

        foreach (var prop in options.GetType().GetProperties())
        {
            if (prop.PropertyType != typeof(string) || !prop.CanRead) continue;
            var name = prop.Name;
            if (name.Contains("Sql", StringComparison.OrdinalIgnoreCase)
                || name.Contains("Query", StringComparison.OrdinalIgnoreCase)
                || name.Contains("Init", StringComparison.OrdinalIgnoreCase))
            {
                yield return prop.GetValue(options) as string;
            }
        }
    }
}
