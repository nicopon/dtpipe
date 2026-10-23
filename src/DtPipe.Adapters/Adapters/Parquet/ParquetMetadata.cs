namespace DtPipe.Adapters.Parquet;

internal static class ParquetMetadata
{
    public const string ComponentName = "parquet";
    public static bool CanHandle(string connectionString) =>
        connectionString.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) &&
        !DtPipe.Adapters.Common.ConnectionUri.HasRemoteScheme(connectionString);
    public const bool SupportsStdio = true;
}
