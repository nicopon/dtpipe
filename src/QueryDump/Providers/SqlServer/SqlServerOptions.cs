using QueryDump.Core.Options;

namespace QueryDump.Providers.SqlServer;

public record SqlServerOptions : IProviderOptions
{
    public static string Prefix => "mssql";
    
    // Placeholder for future options like PacketSize, etc.
}
