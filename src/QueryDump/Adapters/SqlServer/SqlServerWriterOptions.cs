using QueryDump.Core.Attributes;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.SqlServer;

public enum SqlServerInsertMode
{
    Standard,
    Bulk
}

public record SqlServerWriterOptions : IProviderOptions
{
    public static string Prefix => SqlServerConstants.ProviderName;
    public static string DisplayName => "SQL Server Writer Options";

    [CliOption(Description = "Target table name")]
    public string Table { get; set; } = "Export";

    [CliOption(Description = "Data write strategy (Append, Truncate, DeleteThenInsert)")]
    public SqlServerWriteStrategy Strategy { get; set; }

    [CliOption(Description = "Data insert mode (Standard, Bulk)")]
    public SqlServerInsertMode InsertMode { get; set; } = SqlServerInsertMode.Standard;
}

public enum SqlServerWriteStrategy
{
    Append,
    Truncate,
    DeleteThenInsert
}
