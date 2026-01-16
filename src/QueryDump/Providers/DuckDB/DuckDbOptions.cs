using QueryDump.Core.Options;

namespace QueryDump.Providers.DuckDB;

public record DuckDbOptions : IProviderOptions
{
    public static string Prefix => "duck";
    
    // Placeholder for future options
}
