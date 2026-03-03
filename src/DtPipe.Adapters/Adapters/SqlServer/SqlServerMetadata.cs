namespace DtPipe.Adapters.SqlServer;

internal static class SqlServerMetadata
{
    public const string ComponentName = "mssql";

    /// <summary>
    /// Returns false because there is no deterministic way to ensure a connection string
    /// belongs to this provider without a prefix. We group this under an explicit choice
    /// to avoid ambiguity.
    /// </summary>
    public static bool CanHandle(string connectionString) => false;

    public const bool SupportsStdio = false;
}
