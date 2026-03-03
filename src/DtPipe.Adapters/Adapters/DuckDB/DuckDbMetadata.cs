namespace DtPipe.Adapters.DuckDB;

internal static class DuckDbMetadata
{
    public const string ComponentName = "duck";
    public static bool CanHandle(string connectionString) => connectionString.Contains(".duckdb", StringComparison.OrdinalIgnoreCase);
    public const bool SupportsStdio = false;
}
