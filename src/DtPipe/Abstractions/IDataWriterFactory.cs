using DtPipe.Configuration;
using DtPipe.Core.Options;
namespace DtPipe.Core.Abstractions;

/// <summary>
/// Factory for creating data writers for a specific output format.
/// </summary>
public interface IDataWriterFactory : IDataFactory
{
	/// <summary>
	/// Creates a writer for the given options.
	/// </summary>
	IDataWriter Create(DumpOptions options);

	/// <summary>
	/// Gets the option types supported by this writer.
	/// </summary>
	IEnumerable<Type> GetSupportedOptionTypes();
}
