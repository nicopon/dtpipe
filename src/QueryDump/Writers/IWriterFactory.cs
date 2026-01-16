using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Writers;

/// <summary>
/// Factory for creating data writers for a specific output format.
/// </summary>
public interface IWriterFactory : ICliContributor
{
    /// <summary>
    /// The file extension this factory handles (e.g., ".csv", ".parquet").
    /// </summary>
    string SupportedExtension { get; }
    
    /// <summary>
    /// Creates a writer for the given options.
    /// </summary>
    IDataWriter Create(DumpOptions options);
    
    /// <summary>
    /// Gets the option types supported by this writer.
    /// </summary>
    IEnumerable<Type> GetSupportedOptionTypes();
}
