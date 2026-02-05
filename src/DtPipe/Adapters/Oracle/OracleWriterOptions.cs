using DtPipe.Core.Options;
using DtPipe.Cli.Attributes;

namespace DtPipe.Adapters.Oracle;

public enum OracleWriteStrategy
{
    Append,
    Truncate,
    DeleteThenInsert,
    Recreate,
    Upsert,
    Ignore
}

public enum OracleInsertMode
{
    Standard,
    Bulk,
    Append
}

public record OracleWriterOptions : IProviderOptions
{
    public static string Prefix => OracleConstants.ProviderName;
    public static string DisplayName => "Oracle Writer Options";
    public string? Key { get; init; }

    [CliOption(Description = "Target table name")]
    public string Table { get; set; } = "export";

    [CliOption(Description = "Data write strategy (Append, Truncate, DeleteThenInsert)")]
    public OracleWriteStrategy Strategy { get; set; }

    [CliOption(Description = "Data insert mode (Standard, Bulk, Append)")]
    public OracleInsertMode InsertMode { get; set; }


}
