using System;

namespace QueryDump.Adapters.SqlServer;

public static class SqlServerConnectionHelper
{
    private const string Prefix = "mssql:"; // Using mssql: as prefix (common standard)

    public static bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;

        if (connectionString.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return connectionString.Contains("Server=", StringComparison.OrdinalIgnoreCase) 
               || connectionString.Contains("Initial Catalog=", StringComparison.OrdinalIgnoreCase)
               || connectionString.Contains("Integrated Security=", StringComparison.OrdinalIgnoreCase);
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
