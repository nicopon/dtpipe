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
/// <para><see cref="StripComments"/> applies the same reading to a whole text, for a classifier that
/// scans for verbs rather than for the first keyword.</para>
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
                if (!TryReadBlockComment(query, i, out var after, out reason))
                    return null;
                i = after;
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

    /// <summary>
    /// The text with every comment replaced by one space, or the text unchanged when it holds anything
    /// the engines would read differently. Unchanged is the safe answer: a classifier that scans for
    /// verbs then sees more text, never less.
    /// </summary>
    /// <remarks>
    /// Only a construct every engine reads as a comment is removed: <c>/* … */</c> by the rule of
    /// <see cref="LeadingKeyword"/>, and <c>--</c> followed by whitespace or the end of the text (MySQL
    /// reads <c>1--1</c> as arithmetic). A <c>#</c> comment stays as text. The text is left unchanged
    /// when a quoted span cannot be delimited the same way everywhere: a backslash inside a literal
    /// (MySQL, PostgreSQL <c>E''</c>), an Oracle <c>q''</c> literal, a PostgreSQL or DuckDB dollar
    /// quote, a span that is never closed. Quoted spans and identifiers — <c>'…'</c>, <c>"…"</c>,
    /// <c>`…`</c>, <c>[…]</c> — are copied through, so a <c>--</c> inside one is not a comment.
    /// </remarks>
    public static string StripComments(string query)
    {
        var result = new System.Text.StringBuilder(query.Length);
        var i = 0;
        while (i < query.Length)
        {
            var c = query[i];

            if (c == '-' && i + 1 < query.Length && query[i + 1] == '-'
                && (i + 2 >= query.Length || char.IsWhiteSpace(query[i + 2])))
            {
                while (i < query.Length && query[i] != '\n' && query[i] != '\r') i++;
                result.Append(' ');
                continue;
            }

            if (c == '/' && i + 1 < query.Length && query[i + 1] == '*')
            {
                if (!TryReadBlockComment(query, i, out var after, out _))
                    return query;
                i = after;
                result.Append(' ');
                continue;
            }

            if (c is '\'' or '"' or '`' or '[')
            {
                if (c == '\'' && i > 0 && query[i - 1] is 'q' or 'Q')
                    return query;

                var close = c == '[' ? ']' : c;
                var end = i + 1;
                while (true)
                {
                    if (end >= query.Length || query[end] == '\\')
                        return query;
                    if (query[end] == close)
                    {
                        if (end + 1 < query.Length && query[end + 1] == close) { end += 2; continue; }
                        break;
                    }
                    end++;
                }

                result.Append(query, i, end + 1 - i);
                i = end + 1;
                continue;
            }

            if (c == '$' && i + 1 < query.Length && (query[i + 1] == '$' || char.IsLetter(query[i + 1]) || query[i + 1] == '_'))
                return query;

            result.Append(c);
            i++;
        }

        return result.ToString();
    }

    /// <summary>Reads the <c>/* … */</c> comment at <paramref name="start"/>, refusing the shapes the engines read differently.</summary>
    private static bool TryReadBlockComment(string query, int start, out int after, out string reason)
    {
        after = start;
        if (start + 2 < query.Length && query[start + 2] == '!')
        {
            reason = "a leading /*! … */ comment is executed by MySQL, so it is not skipped.";
            return false;
        }

        var end = query.IndexOf("*/", start + 2, StringComparison.Ordinal);
        if (end < 0)
        {
            reason = "a leading /* comment is never closed.";
            return false;
        }
        if (query.IndexOf("/*", start + 2, end - (start + 2), StringComparison.Ordinal) >= 0)
        {
            reason = "a leading comment contains another /*; engines disagree on where it ends.";
            return false;
        }

        after = end + 2;
        reason = "";
        return true;
    }
}
