using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.DuckDB;

public class DuckDbWriterOptions : DbWriterOptions, IProviderOptions
{
	public static string Prefix => DuckDbConstants.ProviderName;
	public static string DisplayName => "DuckDB Writer Options";

	[ComponentOption("--table", Aliases = new[] { "-t" }, Description = "Target table name", Required = true)]
	public string Table { get; set; } = string.Empty;

	[ComponentOption("--strategy", Aliases = new[] { "-s" }, Description = "Data write strategy (Append, Truncate, or Recreate)", Hidden = true)]
	public DuckDbWriteStrategy? Strategy { get; set; }

	[ComponentOption("--duck-init", Description = "SQL executed after connection open (e.g. LOAD azure; SET azure_storage_connection_string='...'). Prefix with @ to load from a file.")]
	public string? InitSql { get; set; }
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
