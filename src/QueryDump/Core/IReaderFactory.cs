using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Core;

/// <summary>
/// Factory for creating database readers for a specific provider.
/// </summary>
public interface IReaderFactory : ICliContributor
{
    /// <summary>
    /// The provider name this factory handles (e.g., "oracle", "sqlserver", "duckdb").
    /// </summary>
    string ProviderName { get; }
    
    /// <summary>
    /// Creates a reader for the given options.
    /// </summary>
    IStreamReader Create(DumpOptions options);
    
    /// <summary>
    /// Gets the option types supported by this reader.
    /// </summary>
    IEnumerable<Type> GetSupportedOptionTypes();
}
