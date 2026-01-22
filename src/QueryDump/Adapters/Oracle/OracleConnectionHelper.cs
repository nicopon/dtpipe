using System;

namespace QueryDump.Adapters.Oracle;

public static class OracleConnectionHelper
{
    private const string Prefix = "oracle:";

    public static bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;

        if (connectionString.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Avoid claiming other providers' connection strings
        if (connectionString.StartsWith("duckdb:", StringComparison.OrdinalIgnoreCase) || 
            connectionString.StartsWith("sqlite:", StringComparison.OrdinalIgnoreCase) ||
            connectionString.StartsWith("sqlserver:", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        // Simple heuristic: Contains "Data Source="
        return connectionString.Contains("Data Source=", StringComparison.OrdinalIgnoreCase); 
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
