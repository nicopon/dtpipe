using System.Text.RegularExpressions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Adapters.Shared.Dialects;

/// <summary>
/// Base class for SQL dialects implementing common behavior.
/// </summary>
public abstract partial class BaseSqlDialect : ISqlDialect
{
    [GeneratedRegex(@"^[a-zA-Z_][a-zA-Z0-9_]*$")]
    private static partial Regex SimpleIdentifierRegex();

    public abstract string Normalize(string identifier);

    public abstract string Quote(string identifier);

    public virtual bool NeedsQuoting(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier)) return false;

        // If it contains non-alphanumeric chars (except underscore), it needs quoting
        if (!SimpleIdentifierRegex().IsMatch(identifier)) return true;

        // Check against reserved keywords (can be overridden by derived classes)
        if (IsReservedKeyword(identifier)) return true;

        // Check case sensitivity requirements of the specific dialect
        if (IsCaseMismatch(identifier)) return true;

        return false;
    }

    protected abstract bool IsReservedKeyword(string identifier);

    /// <summary>
    /// Checks if the identifier's case conflicts with the dialect's default unquoted casing.
    /// </summary>
    protected abstract bool IsCaseMismatch(string identifier);

    /// <inheritdoc />
    public virtual string? TableDiscoveryQuery => null;

    /// <summary>No statement by default: a dialect must opt in to what it can actually enforce.</summary>
    public virtual string? ReadOnlySessionSql => null;

    // ── F9 staged-merge generation ──────────────────────────────────────────

    /// <inheritdoc />
    public virtual string BuildStagingMerge(MergeSpec spec)
    {
        var cols = spec.Columns.Select(c => Quote(c.Name)).ToList();
        var keys = spec.KeyColumns.Select(Quote).ToList();
        var conflictTarget = string.Join(", ", keys);

        var updateSet = string.Join(", ", spec.Columns
            .Where(c => !spec.KeyColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase))
            .Select(c => $"{Quote(c.Name)} = EXCLUDED.{Quote(c.Name)}"));

        var sb = new System.Text.StringBuilder();
        sb.Append($"INSERT INTO {spec.QuotedTargetTable} ({string.Join(", ", cols)}) ");
        sb.Append($"SELECT {string.Join(", ", cols)} FROM {spec.SourceTable} ");

        sb.Append(spec.Mode switch
        {
            MergeMode.Upsert => $"ON CONFLICT ({conflictTarget}) DO UPDATE SET {updateSet}",
            MergeMode.Ignore => $"ON CONFLICT ({conflictTarget}) DO NOTHING",
            _ => "",
        });
        return sb.ToString();
    }

    /// <inheritdoc />
    public virtual string BuildParameterizedConflictClause(IReadOnlyList<string> rawKeys, IReadOnlyList<PipeColumnInfo> columns) => "";

    /// <summary>
    /// SQLite-flavored parameterized conflict clause: ON CONFLICT (keys) DO UPDATE SET
    /// col = excluded.col. Exposed here so the writer keeps only parameter plumbing.
    /// </summary>
    protected static string SqliteConflictClause(IReadOnlyList<string> rawKeys, IReadOnlyList<PipeColumnInfo> columns, Func<string, string> quote)
    {
        var conflictTarget = string.Join(", ", rawKeys.Select(quote));
        var updateSet = string.Join(", ", columns
            .Where(c => !rawKeys.Contains(c.Name, StringComparer.OrdinalIgnoreCase))
            .Select(c =>
            {
                var safe = quote(c.Name);
                return $"{safe} = excluded.{safe}";
            }));
        return $"ON CONFLICT ({conflictTarget}) DO UPDATE SET {updateSet}";
    }
}
