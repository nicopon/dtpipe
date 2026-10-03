namespace DtPipe.Adapters.Common;

/// <summary>
/// The one classifier of a reader's query: it names the statement's first keyword and refuses a
/// query whose keyword is not on the reader's read-only list. Every database reader asks it, with
/// its own extra keywords, so a comment-aware rule exists once.
/// </summary>
/// <remarks>
/// Leading whitespace and plain comments (<c>-- …</c>, <c>/* … */</c>) are skipped, because the server
/// ignores them too. Two shapes are refused instead of skipped: <c>/*!</c>, which MySQL executes, and
/// a <c>/*</c> opened inside a comment, which PostgreSQL nests and MySQL does not — the two engines
/// would disagree on where the comment ends, so the keyword read here could differ from the one run.
/// This is a guard against a mistaken statement, not the defence: sessions are opened read-only
/// where the engine allows it.
/// </remarks>
public static class SqlQueryGuard
{
    private static readonly string[] BaseKeywords = { "SELECT", "WITH" };

    public static void RequireReadOnlyStatement(string query, params string[] additionalAllowedKeywords)
    {
        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentException("Query cannot be empty.", nameof(query));

        var keyword = LeadingKeyword(query, out var reason)
            ?? throw new ArgumentException($"Invalid query format: {reason}", nameof(query));

        var upper = keyword.ToUpperInvariant();
        if (BaseKeywords.Contains(upper) || additionalAllowedKeywords.Contains(upper, StringComparer.OrdinalIgnoreCase))
            return;

        throw new InvalidOperationException(
            $"Only SELECT/WITH queries are allowed. Detected: {upper}. DDL/DML statements are blocked for safety.");
    }

    /// <summary>The first keyword after whitespace and comments, or null with the reason there is none.</summary>
    public static string? LeadingKeyword(string query, out string reason)
    {
        var i = 0;
        while (i < query.Length)
        {
            var c = query[i];
            if (char.IsWhiteSpace(c)) { i++; continue; }

            if (c == '-' && i + 1 < query.Length && query[i + 1] == '-')
            {
                while (i < query.Length && query[i] != '\n' && query[i] != '\r') i++;
                continue;
            }

            if (c == '/' && i + 1 < query.Length && query[i + 1] == '*')
            {
                if (i + 2 < query.Length && query[i + 2] == '!')
                {
                    reason = "a leading /*! … */ comment is executed by MySQL, so it is not skipped.";
                    return null;
                }

                var end = query.IndexOf("*/", i + 2, StringComparison.Ordinal);
                if (end < 0)
                {
                    reason = "a leading /* comment is never closed.";
                    return null;
                }
                if (query.IndexOf("/*", i + 2, end - (i + 2), StringComparison.Ordinal) >= 0)
                {
                    reason = "a leading comment contains another /*; engines disagree on where it ends.";
                    return null;
                }
                i = end + 2;
                continue;
            }

            break;
        }

        var start = i;
        while (i < query.Length && (char.IsLetterOrDigit(query[i]) || query[i] == '_')) i++;
        if (i == start)
        {
            reason = "the query must start with a keyword (SELECT, WITH…), after any whitespace or -- and /* */ comments.";
            return null;
        }

        reason = "";
        return query[start..i];
    }
}
