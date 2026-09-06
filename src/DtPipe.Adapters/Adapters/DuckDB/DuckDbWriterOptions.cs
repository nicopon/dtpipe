using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.DuckDB;

[Description("Writes data to a DuckDB database file.")]
[ComponentHelp(
	usageNotes: "Connection string (minimum keys, not exhaustive): 'duck:path/to/file.duckdb'. Driver: DuckDB.NET — its option set defines the full key vocabulary. In YAML, use 'provider-options' -> 'duck' (or 'duck-writer' when the same job also reads from DuckDB) to set table, strategy, and 'duck-init' — SQL run once after connection open, before schema initialization (e.g. to load extensions or set cloud storage credentials).",
	examples: new[] {
		"main:\n  input: \"<adapter-prefix>:<source>\"\n  output: \"duck:warehouse.duckdb\"\n  provider-options:\n    duck-writer:\n      table: \"sales\"\n      strategy: \"Append\"\n      duck-init: \"LOAD azure; SET azure_storage_connection_string='${{keyring://azure-init}}';\""
	})]
public class DuckDbWriterOptions : DbWriterOptions, IProviderOptions, ITableAwareOptions, IVariantAwareOptions
{
	public static string Prefix => DuckDbConstants.ProviderName;
	public static string DisplayName => "DuckDB Writer Options";

	[ComponentOption("--table", Aliases = new[] { "-t" }, Description = "Target table name", Required = true)]
	public string Table { get; set; } = string.Empty;

	[ComponentOption("--strategy", Aliases = new[] { "-s" }, Description = "Data write strategy", Hidden = true)]
	public DuckDbWriteStrategy? Strategy { get; set; }

	[ComponentOption("--duck-init", Description = "SQL executed after connection open (e.g. LOAD azure; SET azure_storage_connection_string='...'). Prefix with @ to load from a file.")]
	public string? InitSql { get; set; }

	/// <summary>
	/// Selector variant ("mysql" for "duck+mysql:"), set by the router. Not a CLI flag: it is
	/// routing data the adapter must not re-derive from the connection string.
	/// </summary>
	public string? Variant { get; set; }
}

public enum DuckDbWriteStrategy
{
	Append,
	Truncate,
	DeleteThenInsert,
	Recreate,
	Upsert,
	Ignore
}
