namespace DtPipe.Adapters.JsonL;

internal static class JsonLMetadata
{
    public const string ComponentName = "jsonl";
    public static bool CanHandle(string connectionString) =>
        connectionString.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase) &&
        !DtPipe.Adapters.Common.ConnectionUri.HasRemoteScheme(connectionString);
    public const bool SupportsStdio = true;
}
