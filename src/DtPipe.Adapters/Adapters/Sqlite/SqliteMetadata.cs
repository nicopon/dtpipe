namespace DtPipe.Adapters.Sqlite;

internal static class SqliteMetadata
{
    public const string ComponentName = "sqlite";
    public static bool CanHandle(string connectionString) =>
        connectionString.EndsWith(".db", StringComparison.OrdinalIgnoreCase) ||
        connectionString.EndsWith(".sqlite", StringComparison.OrdinalIgnoreCase);
    public const bool SupportsStdio = false;
}
