using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace DtPipe.Cli.Security;

/// <summary>
/// Classifies a SQL query / SQL-bearing option value for destructive verbs and network
/// access. Used by the agent guardrails to enforce a fail-closed default: destructive verbs
/// (DROP/DELETE/TRUNCATE/UPDATE/ALTER/INSERT/ATTACH) and network access (LOAD httpfs/azure,
/// remote read_parquet over http/https/s3/ftp) are denied unless explicitly overridden.
/// </summary>
public interface ISqlSafetyPolicy
{
    /// <summary>Analyze a SQL-ish string and return a classification result.</summary>
    SqlSafetyResult Analyze(string sql, SqlSafetyOptions options);
}

/// <summary>Options controlling which classes of SQL are permitted.</summary>
public sealed class SqlSafetyOptions
{
    /// <summary>Allow destructive SQL verbs (DROP/DELETE/TRUNCATE/UPDATE/ALTER/INSERT/ATTACH).</summary>
    public bool AllowDestructive { get; init; } = false;

    /// <summary>Allow network access (LOAD httpfs/azure, remote read_parquet/read_csv over a URL).</summary>
    public bool AllowNetwork { get; init; } = false;
}

/// <summary>The outcome of a <see cref="ISqlSafetyPolicy.Analyze"/> call.</summary>
public sealed class SqlSafetyResult
{
    /// <summary>True when the SQL is permitted under the given options.</summary>
    public bool Allowed { get; init; } = true;

    /// <summary>Human-readable violation messages (empty when allowed).</summary>
    public IReadOnlyList<string> Violations { get; init; } = Array.Empty<string>();

    /// <summary>The destructive verbs that were detected.</summary>
    public IReadOnlyList<string> DetectedDestructive { get; init; } = Array.Empty<string>();

    /// <summary>Whether a network-access pattern was detected.</summary>
    public bool NetworkDetected { get; init; }

    public static SqlSafetyResult Ok() => new() { Allowed = true };

    public static SqlSafetyResult Blocked(params string[] violations) => new()
    {
        Allowed = false,
        Violations = violations
    };
}

/// <summary>
/// Default SQL safety policy. Default mode is <b>deny</b>: destructive verbs and network
/// access are rejected unless the corresponding allow flag is set. Matching is done on
/// token boundaries and is intentionally conservative to fail closed.
/// </summary>
public sealed class DefaultSqlSafetyPolicy : ISqlSafetyPolicy
{
    private static readonly string[] DestructiveVerbs =
        {
         "TRUNCATE", "DROP", "DELETE", "UPDATE", "ALTER", "ATTACH", "INSERT"
        };

    private static readonly Regex DestructiveRegex = new(
         @"(^|[\s'""(,;=:])(" + string.Join("|", DestructiveVerbs.Select(Regex.Escape)) + @")\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex NetworkRegex = new(
        @"(?i)" +
        @"(load\s+(httpfs|azure))|" +
        @"(read_parquet|read_csv|read_csv_auto|read_json)\s*\(\s*['""]?(https?|s3|ftp|gs)://",
        RegexOptions.Compiled);

    /// <summary>
    /// A bare object-storage URI reaches the network without any SQL around it: the s3/azure
    /// providers turn "input: s3://bucket/key.parquet" into a remote fetch on their own. Scanning
    /// only for read_parquet(...) left that path ungated, so connection strings are matched too.
    /// Limited to the schemes a provider actually dials, so an http link in a comment is not a
    /// false positive.
    /// </summary>
    private static readonly Regex ObjectStorageUriRegex = new(
        @"(?i)(^|[\s'""=:,\[(])(s3a?|azure|az)://",
        RegexOptions.Compiled);

    public SqlSafetyResult Analyze(string sql, SqlSafetyOptions options)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            return SqlSafetyResult.Ok();
        }

        var violations = new List<string>();

        var destructiveMatches = DestructiveRegex.Matches(sql)
             .Select(m => m.Groups[2].Value.ToUpperInvariant())
             .Distinct()
             .OrderBy(v => v)
             .ToList();

        bool networkDetected = NetworkRegex.IsMatch(sql) || ObjectStorageUriRegex.IsMatch(sql);

        if (destructiveMatches.Count > 0 && !options.AllowDestructive)
        {
            violations.Add(
                 "Destructive SQL verb(s) detected: " +
                 string.Join(", ", destructiveMatches) +
                 ". Set --allow-destructive to permit (default: deny).");
        }

        if (networkDetected && !options.AllowNetwork)
        {
            violations.Add(
                 "Network access detected (LOAD httpfs/azure, remote read_parquet/read_csv, " +
                 "or an s3://, azure:// connection string). " +
                 "Set --allow-network to permit (default: deny).");
        }

        if (violations.Count == 0)
        {
            return new SqlSafetyResult
            {
                Allowed = true,
                NetworkDetected = networkDetected,
                DetectedDestructive = destructiveMatches
            };
        }

        return new SqlSafetyResult
        {
            Allowed = false,
            Violations = violations,
            NetworkDetected = networkDetected,
            DetectedDestructive = destructiveMatches
        };
    }
}
