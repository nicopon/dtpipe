namespace DtPipe.Adapters.Csv;

internal static class CsvMetadata
{
    public const string ComponentName = "csv";
    public static bool CanHandle(string connectionString) =>
        connectionString.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) &&
        !DtPipe.Adapters.Common.ConnectionUri.HasRemoteScheme(connectionString);
    public const bool SupportsStdio = true;
}
