using System;

namespace QueryDump.Providers.Sqlite;

public static class SqliteConnectionHelper
{
    private const string Prefix = "sqlite:";

    public static bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;
        
        // Explicit prefix check
        if (connectionString.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Heuristics for file extensions
        return connectionString.EndsWith(".sqlite", StringComparison.OrdinalIgnoreCase)
               || connectionString.EndsWith(".sqlite3", StringComparison.OrdinalIgnoreCase);
    }

    public static string GetConnectionString(string original)
    {
        if (original.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return original[Prefix.Length..];
        }
        return original;
    }

    public static string ToDataSourceConnectionString(string pathOrConnectionString)
    {
        var path = GetConnectionString(pathOrConnectionString);
        
        if (path.Contains("Data Source=", StringComparison.OrdinalIgnoreCase))
        {
            return path;
        }
        
        return $"Data Source={path}";
    }
}
