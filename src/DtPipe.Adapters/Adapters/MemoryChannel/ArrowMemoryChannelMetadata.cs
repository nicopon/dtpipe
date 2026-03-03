namespace DtPipe.Adapters.MemoryChannel;

internal static class ArrowMemoryChannelMetadata
{
    public const string ComponentName = "arrow-memory";
    public static bool CanHandle(string connectionString) => false;
    public const bool SupportsStdio = false;
}
