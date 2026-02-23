using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Sqlite;

public enum SqliteWriteStrategy
{
	Append,
	DeleteThenInsert,
	Truncate,
	Recreate,
	Upsert,
	Ignore
}

public class SqliteWriterOptions : IOptionSet, IKeyAwareOptions
{
	public static string Prefix => "sqlite";
	public static string DisplayName => "SQLite Writer";
	public string? Key { get; set; }

	[ComponentOption(Description = "Target table name", Hidden = true)]
	public string Table { get; set; } = "export";

	[ComponentOption(Description = "Data write strategy (Append, Truncate, or Recreate)", Hidden = true)]
	public SqliteWriteStrategy? Strategy { get; set; }
}
