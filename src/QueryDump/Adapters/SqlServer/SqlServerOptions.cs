using QueryDump.Core.Options;

namespace QueryDump.Adapters.SqlServer;

public record SqlServerOptions : IProviderOptions
{
    public static string Prefix => "mssql";
    public static string DisplayName => "SQL Server Reader";
    
    // Placeholder for future options like PacketSize, etc.
}
