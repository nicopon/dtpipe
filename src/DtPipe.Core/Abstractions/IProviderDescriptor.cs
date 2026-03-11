using DtPipe.Core.Options;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Pure metadata descriptor for a data provider.
/// Decoupled from CLI concerns.
/// </summary>
/// <typeparam name="TService">The type of service to create (IDataWriter, IStreamReader, etc)</typeparam>
public interface IProviderDescriptor<out TService> : IDataFactory
{
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
	/// <param name="serviceProvider">Service provider for resolving dependencies (logger, etc).</param>
	TService Create(string connectionString, object options, IServiceProvider serviceProvider);

	/// <summary>
	/// Must return true if the TService created is an IColumnarStreamReader.
	/// Used by CliStreamReaderFactory to forward YieldsColumnarOutput.
	/// Default is false.
	/// </summary>
	bool YieldsColumnarOutput => false;
}
