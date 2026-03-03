namespace DtPipe.Adapters.Null;

internal static class NullMetadata
{
    public const string ComponentName = "null";

    /// <summary>
    /// The null provider is purely explicit (via prefix or full name).
    /// Falling back to it via heuristics is dangerous as it swallows data silently.
    /// </summary>
    public static bool CanHandle(string connectionString) => false;

    public const bool SupportsStdio = true;
}
