namespace DtPipe.Adapters.Arrow;

internal static class ArrowMetadata
{
    public const string ComponentName = "arrow";
    public static bool CanHandle(string connectionString) =>
        connectionString.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) ||
        connectionString.EndsWith(".ipc", StringComparison.OrdinalIgnoreCase);
    public const bool SupportsStdio = true;
}
