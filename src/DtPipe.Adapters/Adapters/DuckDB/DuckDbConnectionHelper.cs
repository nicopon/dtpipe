using System;

namespace DtPipe.Adapters.DuckDB;

public static class DuckDbConnectionHelper
{
    /// <summary>ADO connection string for an ephemeral in-memory DuckDB instance.</summary>
    public const string InMemoryConnectionString = "Data Source=:memory:;";

    /// <summary>
    /// True for every accepted spelling of "run in memory". The leading colon is optional because
    /// "duck::memory:" and "duck:memory" both reach here as ":memory:" / "memory" once the
    /// selector is stripped, and both are documented spellings.
    /// </summary>
    private static bool IsInMemory(string path) =>
        string.IsNullOrEmpty(path)
        || path.Equals(":memory:", StringComparison.OrdinalIgnoreCase)
        || path.Equals("memory", StringComparison.OrdinalIgnoreCase)
        || path.Equals(":memory", StringComparison.OrdinalIgnoreCase);

    public static bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;

        return connectionString.EndsWith(".duckdb", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Turns an already selector-stripped DuckDB target into an ADO connection string.
    /// <para>
    /// This method never sees a "duck:" or "duck+" prefix: ComponentSelector removes it before any
    /// descriptor is called. It used to re-check both prefixes defensively, which quietly split the
    /// in-memory handling across two code paths — the prefixed one was covered by tests while the
    /// real runtime path was not, so "duck:memory" created a database FILE named "memory".
    /// </para>
    /// </summary>
    public static string GetConnectionString(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return InMemoryConnectionString;

        var path = connectionString.Trim();

        if (IsInMemory(path)) return InMemoryConnectionString;

        if (!path.Contains('=', StringComparison.OrdinalIgnoreCase))
        {
            return $"Data Source={path};";
        }

        return path;
    }
}
