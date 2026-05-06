using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Sqlite;

public class SqliteWriterOptions : DbWriterOptions, IOptionSet
{
	public static string Prefix => "sqlite";
	public static string DisplayName => "SQLite Writer";

	[ComponentOption("--table", Aliases = new[] { "-t" }, Description = "Target table name", Required = true)]
	public string Table { get; set; } = string.Empty;

	[ComponentOption("--strategy", Aliases = new[] { "-s" }, Description = "Data write strategy (Append, Truncate, or Recreate)", Hidden = true)]
	public SqliteWriteStrategy? Strategy { get; set; }
}

public enum SqliteWriteStrategy
{
	Append,
	DeleteThenInsert,
	Truncate,
	Recreate,
	Upsert,
	Ignore
}
