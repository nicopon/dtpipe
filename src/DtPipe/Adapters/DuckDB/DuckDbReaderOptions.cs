using DtPipe.Core.Options;

namespace DtPipe.Adapters.DuckDB;

public record DuckDbReaderOptions : IProviderOptions
{
    public static string Prefix => DuckDbConstants.ProviderName;
    public static string DisplayName => "DuckDB Reader";
    
    // Placeholder for future options
}
