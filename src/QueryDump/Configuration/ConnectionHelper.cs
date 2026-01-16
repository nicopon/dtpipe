using QueryDump.Core;

namespace QueryDump.Configuration;

public static class ConnectionHelper
{
    public static string? ResolveConnection(string? connection, string provider)
    {
        if (connection is not null) return connection;

        // Try to guess env var based on provider if known
        if (string.Equals(provider, "oracle", StringComparison.OrdinalIgnoreCase)) 
            return Environment.GetEnvironmentVariable("ORACLE_CONNECTION_STRING");
        
        if (string.Equals(provider, "sqlserver", StringComparison.OrdinalIgnoreCase)) 
            return Environment.GetEnvironmentVariable("MSSQL_CONNECTION_STRING");
        
        if (string.Equals(provider, "duckdb", StringComparison.OrdinalIgnoreCase)) 
            return Environment.GetEnvironmentVariable("DUCKDB_CONNECTION_STRING");
        
        // If auto or unknown, try all common ones
        return Environment.GetEnvironmentVariable("ORACLE_CONNECTION_STRING") ??
               Environment.GetEnvironmentVariable("MSSQL_CONNECTION_STRING") ??
               Environment.GetEnvironmentVariable("DUCKDB_CONNECTION_STRING");
    }

    public static string ApplyTimeouts(string connection, string provider, int timeoutSeconds)
    {
        if (string.Equals(provider, "oracle", StringComparison.OrdinalIgnoreCase) && 
            !connection.Contains("Connection Timeout", StringComparison.OrdinalIgnoreCase))
        {
            return $"{connection.TrimEnd(';')};Connection Timeout={timeoutSeconds}";
        }
        
        if (string.Equals(provider, "sqlserver", StringComparison.OrdinalIgnoreCase) && 
            !connection.Contains("Connect Timeout", StringComparison.OrdinalIgnoreCase))
        {
            return $"{connection.TrimEnd(';')};Connect Timeout={timeoutSeconds}";
        }

        return connection;
    }
}
