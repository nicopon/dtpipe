namespace DtPipe.Adapters.Parquet;

internal static class ParquetMetadata
{
    public const string ComponentName = "parquet";
    public static bool CanHandle(string connectionString) => connectionString.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase);
    public const bool SupportsStdio = true;
}
