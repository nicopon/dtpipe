namespace DtPipe.Adapters.JsonL;

internal static class JsonLMetadata
{
    public const string ComponentName = "jsonl";
    public static bool CanHandle(string connectionString) => connectionString.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase);
    public const bool SupportsStdio = true;
}
