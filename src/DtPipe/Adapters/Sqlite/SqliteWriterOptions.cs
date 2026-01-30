using System.ComponentModel;
using DtPipe.Core.Options;
using DtPipe.Core.Attributes;

namespace DtPipe.Adapters.Sqlite;

public enum SqliteWriteStrategy
{
    Append,
    DeleteThenInsert,
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
