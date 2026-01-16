using QueryDump.Core.Options;

namespace QueryDump.Providers.DuckDB;

public record DuckDbOptions : IProviderOptions
{
    public static string Prefix => "duck";
    public static string DisplayName => "DuckDB Reader";
    
    // Placeholder for future options
}
