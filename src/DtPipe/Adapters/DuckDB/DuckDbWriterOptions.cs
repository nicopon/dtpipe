using DtPipe.Core.Options;
using DtPipe.Cli.Attributes;

namespace DtPipe.Adapters.DuckDB;

public enum DuckDbWriteStrategy
{
    Append,
    Truncate,
    DeleteThenInsert,
    Recreate,
    Upsert,
    Ignore
}

public record DuckDbWriterOptions : IProviderOptions
{
    public static string Prefix => DuckDbConstants.ProviderName;
    public static string DisplayName => "DuckDB Writer Options";

    [CliOption(Description = "Target table name")]
    public string Table { get; set; } = "Export";

    [CliOption(Description = "Data write strategy (Append, Truncate, or Recreate)")]
    public DuckDbWriteStrategy Strategy { get; set; } = DuckDbWriteStrategy.Append;
    public string? Key { get; init; }
}
