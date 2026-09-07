using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.DuckDB;

[Description("Reads data from a DuckDB database file or in-memory instance.")]
[ComponentHelp(
	usageNotes: "Connection string (minimum keys, not exhaustive): 'duck:path/to/file.duckdb' (or 'duck:memory' for an ephemeral in-memory database). Driver: DuckDB.NET — its option set defines the full key vocabulary. In YAML, use 'provider-options' -> 'duck' for query/table, and 'init-sql' to run SQL once the connection opens and before the query executes (e.g. 'LOAD httpfs' or setting cloud storage credentials). The YAML keys are the ones listed above, which are not always the CLI flag: the init SQL is '--duck-init' on the command line and 'init-sql' in YAML.",
	examples: new[] {
		"main:\n  input: \"duck:warehouse.duckdb\"\n  provider-options:\n    duck:\n      query: \"SELECT * FROM sales\"\n      init-sql: \"LOAD httpfs; SET s3_region='eu-west-1';\"\n  output: \"<adapter-prefix>:<target>\""
	})]
public class DuckDbReaderOptions : QueryableReaderOptions, IProviderOptions, IVariantAwareOptions
{
	public static string Prefix => DuckDbConstants.ProviderName;
	public static string DisplayName => "DuckDB Reader";

	[ComponentOption("--duck-init", Description = "SQL executed after connection open (e.g. LOAD httpfs; SET s3_region='...'). Prefix with @ to load from a file.")]
	public string? InitSql { get; set; }

	/// <summary>
	/// Selector variant ("mysql" for "duck+mysql:"), set by the router. Not a CLI flag: it is
	/// routing data the adapter must not re-derive from the connection string.
	/// </summary>
	public string? Variant { get; set; }
}
