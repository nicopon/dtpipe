namespace DtPipe.Adapters.Checksum;

internal static class ChecksumMetadata
{
    public const string ComponentName = "checksum";

    public static bool CanHandle(string connectionString) => false;

    public const bool SupportsStdio = true;
}
