using QueryDump.Configuration;
using QueryDump.Core.Options;
namespace QueryDump.Core.Abstractions;

/// <summary>
/// Factory for creating database readers for a specific provider.
/// </summary>
public interface IStreamReaderFactory : IDataFactory
{
    /// <summary>
    /// Creates a reader for the given options.
    /// </summary>
    IStreamReader Create(DumpOptions options);
        
    /// <summary>
    /// Gets the option types supported by this reader.
    /// </summary>
    IEnumerable<Type> GetSupportedOptionTypes();

    /// <summary>
    /// Indicates if this factory requires a SQL query to create a reader.
    /// </summary>
    bool RequiresQuery { get; }
}
