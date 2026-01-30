using System;

namespace DtPipe.Adapters.PostgreSQL;

public static class PostgreSqlConnectionHelper
{
    public static bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;

        // Simple heuristic fallback
        return connectionString.Contains("Host=", StringComparison.OrdinalIgnoreCase) ||
               connectionString.StartsWith("postgres://", StringComparison.OrdinalIgnoreCase) ||
               connectionString.StartsWith("postgresql://", StringComparison.OrdinalIgnoreCase);
    }

    public static string GetConnectionString(string connectionString)
    {
        return connectionString;
    }
}
