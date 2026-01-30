using System;

namespace DtPipe.Adapters.DuckDB;

public static class DuckDbConnectionHelper
{
    public static bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;
        
        return connectionString.Contains(".duckdb", StringComparison.OrdinalIgnoreCase);
    }

    public static string GetConnectionString(string connectionString)
    {
        return connectionString;
    }
}
