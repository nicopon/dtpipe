using DtPipe.Core.Options;

namespace DtPipe.Adapters.DuckDB;

public record DuckDbReaderOptions : IProviderOptions, IQueryAwareOptions
{
	public static string Prefix => DuckDbConstants.ProviderName;
	public static string DisplayName => "DuckDB Reader";

	public string? Query { get; set; }
}
