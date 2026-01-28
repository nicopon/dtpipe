using System.ComponentModel;
using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Adapters.Sqlite;

public enum SqliteWriteStrategy
{
    Append,
    Truncate,
    Recreate
}

public class SqliteWriterOptions : IOptionSet
{
    public static string Prefix => "sqlite";
    public static string DisplayName => "SQLite Writer";

    [CliOption(Description = "Target table name")]
    public string Table { get; set; } = "Export";

    [CliOption(Description = "Data write strategy (Append, Truncate, or Recreate)")]
    public SqliteWriteStrategy Strategy { get; set; } = SqliteWriteStrategy.Append;
}
