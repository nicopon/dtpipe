using DtPipe.Core.Options;

namespace DtPipe.Adapters.Sqlite;

public record SqliteReaderOptions : IProviderOptions, IQueryAwareOptions
{
	public static string Prefix => "sqlite";
	public static string DisplayName => "SQLite Reader";

	public string? Query { get; set; }
}
