using DtPipe.Cli.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.SqlServer;

public enum SqlServerInsertMode
{
    Standard,
    Bulk
}

public record SqlServerWriterOptions : IProviderOptions
{
    public static string Prefix => SqlServerConstants.ProviderName;
    public static string DisplayName => "SQL Server Writer Options";
    public string? Key { get; init; }

    [CliOption(Description = "Target table name")]
    public string Table { get; set; } = "export";

    [CliOption(Description = "Data write strategy (Append, Truncate, DeleteThenInsert)")]
    public SqlServerWriteStrategy Strategy { get; set; }

    [CliOption(Description = "Data insert mode (Standard, Bulk)")]
    public SqlServerInsertMode InsertMode { get; set; } = SqlServerInsertMode.Standard;
}

public enum SqlServerWriteStrategy
{
    Append,
    Truncate,
    DeleteThenInsert,
    Recreate,
    Upsert,
    Ignore
}
