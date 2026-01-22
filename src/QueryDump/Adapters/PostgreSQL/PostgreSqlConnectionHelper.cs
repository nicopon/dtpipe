using System;

namespace QueryDump.Adapters.PostgreSQL;

public static class PostgreSqlConnectionHelper
{
    private const string PrefixShort = "postgres:";
    private const string PrefixLong = "postgresql:";

    public static bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;

        if (connectionString.StartsWith(PrefixShort, StringComparison.OrdinalIgnoreCase) ||
            connectionString.StartsWith(PrefixLong, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Heuristic: Match standard Npgsql connection string keywords
        return connectionString.Contains("Host=", StringComparison.OrdinalIgnoreCase) && 
               (connectionString.Contains("Port=", StringComparison.OrdinalIgnoreCase) || connectionString.Contains("User ID=", StringComparison.OrdinalIgnoreCase));
    }

    public static string GetConnectionString(string original)
    {
        if (original.StartsWith(PrefixShort, StringComparison.OrdinalIgnoreCase))
        {
            return original[PrefixShort.Length..];
        }
        if (original.StartsWith(PrefixLong, StringComparison.OrdinalIgnoreCase))
        {
            return original[PrefixLong.Length..];
        }
        return original;
    }
}
