using DtPipe.Core.Attributes;

namespace DtPipe.Adapters.Common;

/// <summary>
/// Base options for database-connected adapters (readers and writers).
/// Declares shared CLI flags so each DB adapter does not need to repeat them.
/// </summary>
public abstract class DbConnectionOptions
{
    [ComponentOption("--connection-timeout", Description = "Connection timeout in seconds")]
    public int ConnectionTimeout { get; set; } = 10;

    [ComponentOption("--query-timeout", Description = "Query timeout in seconds (0 = no timeout)")]
    public int QueryTimeout { get; set; } = 0;

    [ComponentOption("--unsafe-query", Description = "Allow unsafe SQL queries")]
    public bool UnsafeQuery { get; set; } = false;
}
