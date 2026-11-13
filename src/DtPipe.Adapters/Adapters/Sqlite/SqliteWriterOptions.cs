using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Adapters.Shared.Abstractions;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Sqlite;

[Description("Writes data to an SQLite database.")]
[ComponentHelp(
    usageNotes: "Connection string (minimum keys, not exhaustive): 'sqlite:Data Source=path/to/db.db'. Driver: Microsoft.Data.Sqlite — its option set defines the full key vocabulary. In YAML, use 'provider-options' -> 'sqlite' (or 'sqlite-writer' when the same job also reads from SQLite) to set table and write strategy.",
    examples: new[] {
        "main:\n  input: \"<adapter-prefix>:<source>\"\n  output: \"sqlite:Data Source=business.db\"\n  provider-options:\n    sqlite-writer:\n      table: \"orders\"\n      strategy: \"Upsert\"\n      key: \"id\""
    })]
public class SqliteWriterOptions : DbWriterOptions, IOptionSet, ITableAwareOptions
{
    public static string Prefix => "sqlite";
    public static string DisplayName => "SQLite Writer";

    [ComponentOption("--table", Aliases = new[] { "-t" }, Description = "Target table name", Required = true)]
    public string Table { get; set; } = string.Empty;

    [ComponentOption("--strategy", Aliases = new[] { "-s" }, Description = "Data write strategy")]
    public SqliteWriteStrategy? Strategy { get; set; }
}

public enum SqliteWriteStrategy
{
    [Description(WriteStrategyDescriptions.Append)]
    Append,
    [Description(WriteStrategyDescriptions.DeleteThenInsert)]
    DeleteThenInsert,
    [Description(WriteStrategyDescriptions.Truncate)]
    Truncate,
    [Description(WriteStrategyDescriptions.Recreate)]
    Recreate,
    [Description(WriteStrategyDescriptions.Upsert)]
    Upsert,
    [Description(WriteStrategyDescriptions.Ignore)]
    Ignore
}
