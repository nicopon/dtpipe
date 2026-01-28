using System.ComponentModel;
using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Adapters.Oracle;

public enum OracleWriteStrategy
{
    Append,
    Truncate,
    DeleteThenInsert
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

    [CliOption(Description = "Target table name")]
    public string Table { get; set; } = "Export";

    [CliOption(Description = "Data write strategy (Append, Truncate, DeleteThenInsert)")]
    public OracleWriteStrategy Strategy { get; set; }

    [CliOption(Description = "Data insert mode (Standard, Bulk, Append)")]
    public OracleInsertMode InsertMode { get; set; }


}
