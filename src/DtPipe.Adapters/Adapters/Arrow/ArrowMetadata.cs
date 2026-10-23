namespace DtPipe.Adapters.Arrow;

internal static class ArrowMetadata
{
    public const string ComponentName = "arrow";
    public static bool CanHandle(string connectionString) =>
        (connectionString.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) ||
         connectionString.EndsWith(".ipc", StringComparison.OrdinalIgnoreCase)) &&
        !DtPipe.Adapters.Common.ConnectionUri.HasRemoteScheme(connectionString);
    public const bool SupportsStdio = true;
}
