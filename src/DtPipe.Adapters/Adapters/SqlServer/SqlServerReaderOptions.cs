using DtPipe.Core.Options;

namespace DtPipe.Adapters.SqlServer;

public record SqlServerReaderOptions : IProviderOptions, IQueryAwareOptions
{
	public static string Prefix => SqlServerConstants.ProviderName;
	public static string DisplayName => "SQL Server Reader";

	// Placeholder for future options like PacketSize, etc.
	public string? Query { get; set; }
}
