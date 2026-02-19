using DtPipe.Core.Options;

namespace DtPipe.Adapters.PostgreSQL;

public record PostgreSqlReaderOptions : IProviderOptions, IQueryAwareOptions
{
	public static string Prefix => PostgreSqlConstants.ProviderName;
	public static string DisplayName => "PostgreSQL Reader";

	public string? Query { get; set; }
}
