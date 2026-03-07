using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.DuckDB;

public enum DuckDbWriteStrategy
{
	Append,
	Truncate,
	DeleteThenInsert,
	Recreate,
	Upsert,
	Ignore
}

public record DuckDbWriterOptions : IProviderOptions, IKeyAwareOptions
{
	public static string Prefix => DuckDbConstants.ProviderName;
	public static string DisplayName => "DuckDB Writer Options";

	[ComponentOption(Description = "Target table name", Required = true)]
	public string Table { get; set; } = string.Empty;

	[ComponentOption(Description = "Data write strategy (Append, Truncate, or Recreate)", Hidden = true)]
	public DuckDbWriteStrategy? Strategy { get; set; }
	public string? Key { get; set; }
}
