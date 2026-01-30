using System.ComponentModel;
using DtPipe.Core.Options;
using DtPipe.Core.Attributes;

namespace DtPipe.Adapters.Oracle;

public enum OracleWriteStrategy
{
    Append,
    Truncate,
    DeleteThenInsert,
    Recreate
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
