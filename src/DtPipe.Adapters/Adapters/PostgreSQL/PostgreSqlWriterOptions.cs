using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.PostgreSQL;

public class PostgreSqlWriterOptions : DbWriterOptions, IWriterOptions
{
	public static string Prefix => PostgreSqlConstants.ProviderName;
	public static string DisplayName => "PostgreSQL Writer Options";

	[ComponentOption("--table", Aliases = new[] { "-t" }, Description = "Target table name", Required = true)]
	public string Table { get; set; } = string.Empty;

	[ComponentOption("--strategy", Aliases = new[] { "-s" }, Description = "Write strategy: Append, Truncate, or DeleteThenInsert", Hidden = true)]
	public PostgreSqlWriteStrategy? Strategy { get; set; }

	[ComponentOption("--insert-mode", Description = "Data insert mode (Standard, Bulk)", Hidden = true)]
	public PostgreSqlInsertMode? InsertMode { get; set; }
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
	DeleteThenInsert,
	Recreate,
	Upsert,
	Ignore
}
