using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.DuckDB;

public class DuckDbReaderOptions : QueryableReaderOptions, IProviderOptions
{
	public static string Prefix => DuckDbConstants.ProviderName;
	public static string DisplayName => "DuckDB Reader";

	[ComponentOption("--duck-init", Description = "SQL executed after connection open (e.g. LOAD httpfs; SET s3_region='...'). Prefix with @ to load from a file.")]
	public string? InitSql { get; set; }
}
