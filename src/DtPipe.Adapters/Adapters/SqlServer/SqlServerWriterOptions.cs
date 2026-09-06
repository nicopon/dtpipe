using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.SqlServer;

[Description("Writes data to a SQL Server database.")]
[ComponentHelp(
	usageNotes: "Connection string (minimum keys, not exhaustive): 'mssql:Server=host;Database=db;User Id=user;Password=pass;TrustServerCertificate=True'. Driver: Microsoft.Data.SqlClient — its option set defines the full key vocabulary. In YAML, use 'provider-options' -> 'mssql' (or 'mssql-writer' when the same job also reads from SQL Server) to set table, strategy, and insert mode.",
	examples: new[] {
		"main:\n  input: \"<adapter-prefix>:<source>\"\n  output: \"mssql:Server=.;Database=mydb;User Id=sa;Password=pass;TrustServerCertificate=True\"\n  provider-options:\n    mssql-writer:\n      table: \"dbo.Orders\"\n      strategy: \"Upsert\"\n      key: \"OrderId\""
	})]
public class SqlServerWriterOptions : DbWriterOptions, IProviderOptions, ITableAwareOptions
{
	public static string Prefix => SqlServerConstants.ProviderName;
	public static string DisplayName => "SQL Server Writer Options";

	[ComponentOption("--table", Aliases = new[] { "-t" }, Description = "Target table name", Required = true)]
	public string Table { get; set; } = string.Empty;

	[ComponentOption("--strategy", Aliases = new[] { "-s" }, Description = "Data write strategy", Hidden = true)]
	public SqlServerWriteStrategy? Strategy { get; set; }

	[ComponentOption("--insert-mode", Description = "Data insert mode", Hidden = true)]
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
