using QueryDump.Core.Options;
using QueryDump.Configuration;

namespace QueryDump.Core.Abstractions;

/// <summary>
/// Pure metadata descriptor for a data provider. 
/// Decoupled from CLI concerns.
/// </summary>
/// <typeparam name="TService">The type of service to create (IDataWriter, IStreamReader, etc)</typeparam>
public interface IProviderDescriptor<out TService>
{
    string ProviderName { get; }
    Type OptionsType { get; }
    
    bool CanHandle(string connectionString);

    /// <summary>
    /// Indicates if this provider requires a SQL query to read data.
    /// True for DBs (Oracle, SQL Server), False for Files (CSV, Parquet) or Generators (Sample).
    /// </summary>
    bool RequiresQuery { get; }

    /// <summary>
    /// Creates the service instance.
    /// </summary>
    /// <param name="connectionString">The resolved connection string (potentially stripped of prefix).</param>
    /// <param name="options">The bound options object (of type OptionsType).</param>
    /// <param name="context">The full DumpOptions context (provides Query, Timeout, etc).</param>
    /// <param name="serviceProvider">Service provider for resolving dependencies (logger, etc).</param>
    TService Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider);
}
