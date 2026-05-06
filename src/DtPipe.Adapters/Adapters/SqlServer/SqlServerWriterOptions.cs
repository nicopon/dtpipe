using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.SqlServer;

public class SqlServerWriterOptions : DbWriterOptions, IProviderOptions
{
	public static string Prefix => SqlServerConstants.ProviderName;
	public static string DisplayName => "SQL Server Writer Options";

	[ComponentOption("--table", Aliases = new[] { "-t" }, Description = "Target table name", Required = true)]
	public string Table { get; set; } = string.Empty;

	[ComponentOption("--strategy", Aliases = new[] { "-s" }, Description = "Data write strategy (Append, Truncate, DeleteThenInsert)", Hidden = true)]
	public SqlServerWriteStrategy? Strategy { get; set; }

	[ComponentOption("--insert-mode", Description = "Data insert mode (Standard, Bulk)", Hidden = true)]
	public SqlServerInsertMode? InsertMode { get; set; }
}

public enum SqlServerInsertMode
{
	Standard,
	Bulk
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
