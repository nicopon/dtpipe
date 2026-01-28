using QueryDump.Core.Attributes;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.PostgreSQL;

public class PostgreSqlWriterOptions : IWriterOptions
{
    public static string Prefix => PostgreSqlConstants.ProviderName;
    public static string DisplayName => "PostgreSQL Writer Options";

    // Writer Options
    [CliOption(Description = "Target table name")]
    public string Table { get; set; } = "Export";

    [CliOption(Description = "Write strategy: Append, Truncate, or DeleteThenInsert")]
    public PostgreSqlWriteStrategy Strategy { get; set; } = PostgreSqlWriteStrategy.Append;

    [CliOption(Description = "Data insert mode (Standard, Bulk)")]
    public PostgreSqlInsertMode InsertMode { get; set; } = PostgreSqlInsertMode.Standard;
}

public enum PostgreSqlInsertMode
{
    Standard,
    Bulk
}

public enum PostgreSqlWriteStrategy
{
    Append,
    Truncate,
    DeleteThenInsert
}

