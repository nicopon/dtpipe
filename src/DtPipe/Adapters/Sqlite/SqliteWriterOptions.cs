using DtPipe.Core.Options;
using DtPipe.Cli.Attributes;

namespace DtPipe.Adapters.Sqlite;

public enum SqliteWriteStrategy
{
    Append,
    DeleteThenInsert,
    Recreate,
    Upsert,
    Ignore
}

public class SqliteWriterOptions : IOptionSet
{
    public static string Prefix => "sqlite";
    public static string DisplayName => "SQLite Writer";
    public string? Key { get; init; }

    [CliOption(Description = "Target table name", Hidden = true)]
    public string Table { get; set; } = "export";

    [CliOption(Description = "Data write strategy (Append, Truncate, or Recreate)", Hidden = true)]
    public SqliteWriteStrategy? Strategy { get; set; }
}
