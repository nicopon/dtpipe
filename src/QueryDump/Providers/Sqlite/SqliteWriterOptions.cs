using System.ComponentModel;
using QueryDump.Core.Options;

namespace QueryDump.Providers.Sqlite;

public class SqliteWriterOptions : IOptionSet
{
    public static string Prefix => "sqlite";
    public static string DisplayName => "SQLite Writer";

    [Description("Target table name for SQLite export")]
    public string Table { get; set; } = "Export";

    [Description("Write strategy: Append, Truncate, or Recreate")]
    public string Strategy { get; set; } = "Append";
}
