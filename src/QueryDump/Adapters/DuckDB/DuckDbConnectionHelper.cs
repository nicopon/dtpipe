using System;

namespace QueryDump.Adapters.DuckDB;

public static class DuckDbConnectionHelper
{
    private const string Prefix = "duckdb:";

    public static bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;
        
        // Explicit prefix check
        if (connectionString.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Heuristics for backward compatibility / user convenience
        // DuckDB often just takes a file path or usage of specific keys
        return connectionString.EndsWith(".duckdb", StringComparison.OrdinalIgnoreCase)
               || connectionString.EndsWith(".db", StringComparison.OrdinalIgnoreCase)
               || (connectionString.Contains("DataSource=", StringComparison.OrdinalIgnoreCase) && connectionString.Contains(".duckdb", StringComparison.OrdinalIgnoreCase));
    }

    public static string GetConnectionString(string original)
    {
        if (original.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return original[Prefix.Length..];
        }
        return original;
    }
}
