using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Common;

/// <summary>
/// Base options for database reader adapters that execute SQL queries.
/// </summary>
public abstract class QueryableReaderOptions : DbConnectionOptions, IQueryAwareOptions
{
    [ComponentOption("--query", Aliases = new[] { "-q" }, Description = "SQL query or file path")]
    public string? Query { get; set; }

    [ComponentOption("--table", Aliases = new[] { "-t" }, Description = "Source table name (auto-builds SELECT * FROM if --query is not provided)")]
    public string? Table { get; set; }
}
