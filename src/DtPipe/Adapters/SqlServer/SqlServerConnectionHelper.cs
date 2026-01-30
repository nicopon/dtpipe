using System;

namespace DtPipe.Adapters.SqlServer;

public static class SqlServerConnectionHelper
{


    public static bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;

        // Fallback heuristic
        return connectionString.Contains("Data Source=", StringComparison.OrdinalIgnoreCase) ||
               connectionString.Contains("Server=", StringComparison.OrdinalIgnoreCase);
    }

    public static string GetConnectionString(string connectionString)
    {
        return connectionString;
    }
}
