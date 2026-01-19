using System;

namespace QueryDump.Providers.Oracle;

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
