using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.PostgreSQL;

[Description("Writes data to a PostgreSQL database.")]
[ComponentHelp(
	usageNotes: "Connection string (minimum keys, not exhaustive): 'pg:Host=host;Port=port;Database=db;Username=user;Password=pass'. Driver: Npgsql — its option set defines the full key vocabulary. In YAML, use 'provider-options' -> 'pg' (or 'pg-writer' when the same job also reads from PostgreSQL) to set table, strategy, and insert mode.",
	examples: new[] {
		"main:\n  input: \"<adapter-prefix>:<source>\"\n  output: \"pg:Host=localhost;Database=prod;Username=postgres\"\n  provider-options:\n    pg-writer:\n      table: \"public.orders\"\n      strategy: \"Upsert\"\n      key: \"order_id\""
	})]
public class PostgreSqlWriterOptions : DbWriterOptions, IWriterOptions, ITableAwareOptions
{
	public static string Prefix => PostgreSqlConstants.ProviderName;
	public static string DisplayName => "PostgreSQL Writer Options";

	[ComponentOption("--table", Aliases = new[] { "-t" }, Description = "Target table name", Required = true)]
	public string Table { get; set; } = string.Empty;

	[ComponentOption("--strategy", Aliases = new[] { "-s" }, Description = "Write strategy", Hidden = true)]
	public PostgreSqlWriteStrategy? Strategy { get; set; }

	[ComponentOption("--insert-mode", Description = "Data insert mode", Hidden = true)]
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
