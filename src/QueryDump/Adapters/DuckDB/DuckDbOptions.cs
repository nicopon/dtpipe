using QueryDump.Core.Options;

namespace QueryDump.Adapters.DuckDB;

public record DuckDbOptions : IProviderOptions
{
    public static string Prefix => "duck";
    public static string DisplayName => "DuckDB Reader";
    
    // Placeholder for future options
}
