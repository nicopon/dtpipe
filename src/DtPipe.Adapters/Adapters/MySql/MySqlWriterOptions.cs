using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.MySql;

[Description("Writes data to a MySQL or MariaDB database.")]
[ComponentHelp(
	usageNotes: "Connection string (minimum keys, not exhaustive): 'mysql:Server=host;Port=3306;Database=db;User ID=user;Password=pass'. Upsert uses INSERT ... ON DUPLICATE KEY UPDATE, which fires on the table's PRIMARY KEY or UNIQUE index; when no such index covers the key columns the writer falls back to DELETE+INSERT and says so. The Bulk insert mode needs LOAD DATA LOCAL INFILE, i.e. local_infile=ON server-side; otherwise it degrades to batched multi-row INSERT. Driver: MySqlConnector — its option set defines the full key vocabulary, and is not identical to MySql.Data's. In YAML, use 'provider-options' -> 'mysql' (or 'mysql-writer' when the same job also reads from MySQL) to set table, strategy, and insert mode.",
	examples: new[] {
		"main:\n  input: \"<adapter-prefix>:<source>\"\n  output: \"mysql:Server=localhost;Database=prod;User ID=root;Password=pass\"\n  provider-options:\n    mysql-writer:\n      table: \"orders\"\n      strategy: \"Upsert\"\n      key: \"order_id\""
	})]
public class MySqlWriterOptions : DbWriterOptions, IWriterOptions, ITableAwareOptions
{
	public static string Prefix => MySqlConstants.ProviderName;
	public static string DisplayName => "MySQL Writer Options";

	[ComponentOption("--table", Aliases = new[] { "-t" }, Description = "Target table name", Required = true)]
	public string Table { get; set; } = string.Empty;

	[ComponentOption("--strategy", Aliases = new[] { "-s" }, Description = "Write strategy", Hidden = true)]
	public MySqlWriteStrategy? Strategy { get; set; }

	[ComponentOption("--insert-mode", Description = "Data insert mode", Hidden = true)]
	public MySqlInsertMode? InsertMode { get; set; }
}

public enum MySqlInsertMode
{
	Standard,
	Bulk
}

public enum MySqlWriteStrategy
{
	Append,
	Truncate,
	DeleteThenInsert,
	Recreate,
	Upsert,
	Ignore
}
