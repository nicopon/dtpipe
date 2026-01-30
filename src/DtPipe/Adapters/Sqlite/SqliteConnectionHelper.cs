using System;

namespace DtPipe.Adapters.Sqlite;

public static class SqliteConnectionHelper
{


    public static bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;
        


        // Heuristics for file extensions
        return connectionString.EndsWith(".sqlite", StringComparison.OrdinalIgnoreCase)
               || connectionString.EndsWith(".sqlite3", StringComparison.OrdinalIgnoreCase);
    }

    public static string GetConnectionString(string original)
    {
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
