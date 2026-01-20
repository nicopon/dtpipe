using System;

namespace QueryDump.Providers.PostgreSQL;

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

        // Heuristic: Npgsql usually uses "Host=" or "Server=" and "Port=" or "User ID="
        // But to be safe and avoid conflicts (SQL Server uses Server= too), we rely on explicit prefix mostly.
        // Or if it contains specific PostgreSQL keywords like "searchpath="?
        // We'll stick to prefix for auto-detection in this tool, or weak heuristic if needed.
        // NpgsqlBuilder.ConnectionString ? 
        
        // Let's support "Host=" AND "Port=" combination as strong indicator?
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
