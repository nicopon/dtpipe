namespace DtPipe.Adapters.Checksum;

internal static class ChecksumMetadata
{
    public const string ComponentName = "checksum";

    public static bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;
        return connectionString.EndsWith(".checksum", System.StringComparison.OrdinalIgnoreCase)
            && !DtPipe.Adapters.Common.ConnectionUri.HasRemoteScheme(connectionString);
    }

    public const bool SupportsStdio = true;
}
