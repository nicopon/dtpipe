namespace DtPipe.Adapters.MemoryChannel;

internal static class MemoryChannelMetadata
{
    public const string ComponentName = "mem";

    public static bool CanHandle(string connectionString) => false;

    public const bool SupportsStdio = false;
}
