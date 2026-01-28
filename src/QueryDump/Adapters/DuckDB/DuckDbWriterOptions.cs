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
    public static string Prefix => DuckDbConstants.ProviderName;
    public static string DisplayName => "DuckDB Writer Options";

    [CliOption(Description = "Target table name")]
    public string Table { get; set; } = "Export";

    [CliOption(Description = "Data write strategy (Append, Truncate, or Recreate)")]
    public DuckDbWriteStrategy Strategy { get; set; } = DuckDbWriteStrategy.Append;
}
