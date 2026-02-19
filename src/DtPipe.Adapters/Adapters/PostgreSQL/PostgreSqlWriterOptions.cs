using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.PostgreSQL;

public class PostgreSqlWriterOptions : IWriterOptions, IKeyAwareOptions
{
	public static string Prefix => PostgreSqlConstants.ProviderName;
	public static string DisplayName => "PostgreSQL Writer Options";
	public string? Key { get; set; }

	// Writer Options
	[CliOption(Description = "Target table name", Hidden = true)]
	public string Table { get; set; } = "export";

	[CliOption(Description = "Write strategy: Append, Truncate, or DeleteThenInsert", Hidden = true)]
	public PostgreSqlWriteStrategy? Strategy { get; set; }

	[CliOption(Description = "Data insert mode (Standard, Bulk)", Hidden = true)]
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
