using QueryDump.Core.Options;

namespace QueryDump.Adapters.SqlServer;

public record SqlServerReaderOptions : IProviderOptions
{
    public static string Prefix => SqlServerConstants.ProviderName;
    public static string DisplayName => "SQL Server Reader";
    
    // Placeholder for future options like PacketSize, etc.
}
