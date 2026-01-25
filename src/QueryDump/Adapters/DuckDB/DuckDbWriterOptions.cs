using System.ComponentModel;
using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Adapters.DuckDB;

public enum DuckDbWriteStrategy
{
    Append,
    Truncate,
    Recreate
}

public record DuckDbWriterOptions : IProviderOptions
{
    public static string Prefix => "duckdb";
    public static string DisplayName => "DuckDB Writer Options";

    [CliOption("--duck-table", Description = "Target table name. Defaults to 'Export'.")]
    public string Table { get; set; } = "Export";

    [CliOption("--duck-strategy", Description = "Strategy for writing data: Append (default), Truncate, or Recreate.")]
    public DuckDbWriteStrategy Strategy { get; set; } = DuckDbWriteStrategy.Append;
}
