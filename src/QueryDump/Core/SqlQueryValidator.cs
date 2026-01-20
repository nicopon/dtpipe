namespace QueryDump.Core;

/// <summary>
/// Validates SQL queries for dangerous operations.
/// Strict mode: blocks queries containing DDL/DML keywords (INSERT, UPDATE, DELETE, DROP, etc.)
/// Can be bypassed with --unsafe-query for advanced users who know what they're doing.
/// </summary>
public static class SqlQueryValidator
{
    // Keywords that indicate potentially dangerous operations
    private static readonly string[] DangerousKeywords =
    [
        "INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", 
        "CREATE", "GRANT", "REVOKE", "EXEC", "EXECUTE", "MERGE",
        "CALL", "SAVEPOINT", "ROLLBACK", "COMMIT", "SET ",
        "DBMS_", "UTL_", "XP_", "SP_"  // Oracle/SQL Server procedures
    ];

    // Additional patterns that are suspicious
    private static readonly string[] SuspiciousPatterns =
    [
        ";",         // Multiple statements
        "--",        // SQL comments (could hide malicious code)
        "/*",        // Block comments
        "INTO ",     // SELECT INTO
        "OUTFILE",   // File operations
        "DUMPFILE",  // File operations
        "LOAD_FILE"  // File operations
    ];

    /// <summary>
    /// Validates a SQL query for safety.
    /// Throws InvalidOperationException if dangerous keywords are detected.
    /// </summary>
    /// <param name="query">The SQL query to validate.</param>
    /// <param name="unsafeMode">If true, skips validation (bypass for power users).</param>
    /// <exception cref="InvalidOperationException">Thrown when dangerous keywords are detected.</exception>
    public static void Validate(string query, bool unsafeMode = false)
    {
        if (unsafeMode)
        {
            Console.Error.WriteLine("Warning: --unsafe-query enabled. SQL validation bypassed.");
            return;
        }

        if (string.IsNullOrWhiteSpace(query))
        {
            throw new InvalidOperationException("Query cannot be empty.");
        }

        var upperQuery = query.ToUpperInvariant();

        // Check for dangerous keywords
        foreach (var keyword in DangerousKeywords)
        {
            if (ContainsKeyword(upperQuery, keyword))
            {
                throw new InvalidOperationException(
                    $"Query blocked: contains dangerous keyword '{keyword}'. " +
                    "Only SELECT queries are allowed. Use --unsafe-query to bypass this check (at your own risk).");
            }
        }

        // Check for suspicious patterns
        foreach (var pattern in SuspiciousPatterns)
        {
            if (upperQuery.Contains(pattern, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Query blocked: contains suspicious pattern '{pattern}'. " +
                    "Use --unsafe-query to bypass this check (at your own risk).");
            }
        }

        // Verify query starts with SELECT or WITH (for CTEs)
        var trimmedQuery = query.TrimStart();
        if (!trimmedQuery.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase) &&
            !trimmedQuery.StartsWith("WITH", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "Query must start with SELECT or WITH. " +
                "Use --unsafe-query to bypass this check (at your own risk).");
        }
    }

    /// <summary>
    /// Checks if the query contains a keyword as a separate word (not part of another identifier).
    /// </summary>
    private static bool ContainsKeyword(string upperQuery, string keyword)
    {
        var index = 0;
        while ((index = upperQuery.IndexOf(keyword, index, StringComparison.Ordinal)) >= 0)
        {
            // Check if it's a standalone keyword (not part of identifier)
            var before = index == 0 || !char.IsLetterOrDigit(upperQuery[index - 1]) && upperQuery[index - 1] != '_';
            var afterIndex = index + keyword.Length;
            var after = afterIndex >= upperQuery.Length || !char.IsLetterOrDigit(upperQuery[afterIndex]) && upperQuery[afterIndex] != '_';

            if (before && after)
            {
                return true;
            }
            index++;
        }
        return false;
    }
}
