using System.Text.RegularExpressions;

namespace QueryDump.Core;

public enum DatabaseProvider
{
    Oracle,
    SqlServer,
    Postgres,
    MySql,
    Sqlite,
    DuckDB,
    Unknown
}

/// <summary>
/// Proposes a database provider based on the connection string.
/// </summary>
public static partial class ProviderDetector
{
    public static DatabaseProvider Detect(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            return DatabaseProvider.Unknown;

        // DuckDB
        // "Data Source=file.db" (DuckDB.NET style)
        // Check for specific DuckDB keys or file extensions if typical keys are present
        if (Regex.IsMatch(connectionString, @"\.(duckdb|db)$", RegexOptions.IgnoreCase) || 
            connectionString.Contains("DuckDB", StringComparison.OrdinalIgnoreCase))
        {
             // Simple "Data Source=..." check is ambiguous with SQLite/Oracle, 
             // but if it points to a .duckdb file it's a strong hint.
             if (Regex.IsMatch(connectionString, @"\.duckdb", RegexOptions.IgnoreCase))
                return DatabaseProvider.DuckDB;
        }

        // Postgres
        if (connectionString.Contains("Port=5432", StringComparison.OrdinalIgnoreCase) ||
            connectionString.StartsWith("postgres://", StringComparison.OrdinalIgnoreCase) ||
            connectionString.Contains("User ID=", StringComparison.OrdinalIgnoreCase) && connectionString.Contains("Password=", StringComparison.OrdinalIgnoreCase) && !connectionString.Contains("Data Source", StringComparison.OrdinalIgnoreCase))
        {
            // Postgres often uses User ID/Password without Data Source (Host/Server instead)
            // But ADO.NET Npgsql uses "Host" or "Server"
            if (connectionString.Contains("Host=", StringComparison.OrdinalIgnoreCase) || 
                connectionString.Contains("Server=", StringComparison.OrdinalIgnoreCase))
            {
                if (connectionString.Contains("Port=5432", StringComparison.OrdinalIgnoreCase)) 
                    return DatabaseProvider.Postgres;
            }
        }

        // SQL Server
        // "Data Source" or "Server", "Initial Catalog" or "Database", "Integrated Security" or "Trusted_Connection"
        if (connectionString.Contains("Initial Catalog", StringComparison.OrdinalIgnoreCase) ||
            connectionString.Contains("Integrated Security", StringComparison.OrdinalIgnoreCase) ||
            connectionString.Contains("TrustServerCertificate", StringComparison.OrdinalIgnoreCase) ||
            (connectionString.Contains("Server=", StringComparison.OrdinalIgnoreCase) && connectionString.Contains("Database=", StringComparison.OrdinalIgnoreCase) && !connectionString.Contains("Port=5432"))) // Exclude Postgres
        {
            return DatabaseProvider.SqlServer;
        }

        // Oracle
        // "Data Source" + "User Id" (often distinct from generic Server behavior)
        // TNS: (DESCRIPTION=...)
        if (connectionString.Contains("(DESCRIPTION=", StringComparison.OrdinalIgnoreCase) ||
            (connectionString.Contains("Data Source", StringComparison.OrdinalIgnoreCase) && connectionString.Contains("User Id", StringComparison.OrdinalIgnoreCase) && !connectionString.Contains("Initial Catalog", StringComparison.OrdinalIgnoreCase)))
        {
            return DatabaseProvider.Oracle;
        }
        
        // SQLite
        if (connectionString.Contains("Data Source", StringComparison.OrdinalIgnoreCase) && 
            (connectionString.EndsWith(".db") || connectionString.EndsWith(".sqlite") || connectionString.Contains(".db;") || connectionString.Contains("Version=3")))
        {
             return DatabaseProvider.Sqlite;
        }

        // MySQL / MariaDB
        if (connectionString.Contains("Port=3306", StringComparison.OrdinalIgnoreCase) ||
            (connectionString.Contains("Uid=", StringComparison.OrdinalIgnoreCase) && connectionString.Contains("Pwd=", StringComparison.OrdinalIgnoreCase)))
        {
            return DatabaseProvider.MySql;
        }

        // Fallback for DuckDB if simpler checks failed but looks like file source
        if (connectionString.StartsWith("Data Source=", StringComparison.OrdinalIgnoreCase) && 
            (connectionString.Contains(".duckdb", StringComparison.OrdinalIgnoreCase)))
        {
            return DatabaseProvider.DuckDB;
        }

        return DatabaseProvider.Unknown;
    }
}
