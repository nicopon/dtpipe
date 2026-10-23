namespace DtPipe.Adapters.DuckDB;

internal static class DuckDbMetadata
{
    public const string ComponentName = "duck";

    /// <summary>
    /// Content check only. CanHandle receives the RAW connection string, so testing for our own
    /// "duck:"/"duck+" prefix here would claim strings the router has not stripped and hand the
    /// provider a dirty value — see the warning on IDataFactory.CanHandle. Selector routing
    /// ("duck:", "duck+mysql:") is ComponentSelector's job.
    /// </summary>
    public static bool CanHandle(string connectionString) =>
        connectionString.Contains(".duckdb", StringComparison.OrdinalIgnoreCase);

    public const bool SupportsStdio = false;
}
