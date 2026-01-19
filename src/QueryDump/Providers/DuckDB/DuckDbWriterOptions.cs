using System.ComponentModel;
using QueryDump.Core.Options;

namespace QueryDump.Providers.DuckDB;

public enum DuckDbWriteStrategy
{
    Append,
    Truncate,
    Recreate
}

public record DuckDbWriterOptions : IProviderOptions
{
    public static string Prefix => "duckdb-writer";
    public static string DisplayName => "DuckDB Writer Options";

    [Description("Target table name. Defaults to 'Export'.")]
    public string Table { get; set; } = "Export";

    [Description("Strategy for writing data: Append (default), Truncate, or Recreate.")]
    public DuckDbWriteStrategy Strategy { get; set; } = DuckDbWriteStrategy.Append;
}
