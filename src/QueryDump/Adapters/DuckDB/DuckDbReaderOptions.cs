using QueryDump.Core.Options;

namespace QueryDump.Adapters.DuckDB;

public record DuckDbReaderOptions : IProviderOptions
{
    public static string Prefix => DuckDbConstants.ProviderName;
    public static string DisplayName => "DuckDB Reader";
    
    // Placeholder for future options
}
