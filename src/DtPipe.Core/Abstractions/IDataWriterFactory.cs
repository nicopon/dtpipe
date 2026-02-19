using DtPipe.Core.Options;
namespace DtPipe.Core.Abstractions;

/// <summary>
/// Factory for creating data writers for a specific output format.
/// </summary>
public interface IDataWriterFactory : IDataFactory
{
	/// <summary>
	/// Creates a writer for the given options registry.
	/// </summary>
	IDataWriter Create(OptionsRegistry registry);

	/// <summary>
	/// Gets the option types supported by this writer.
	/// </summary>
	IEnumerable<Type> GetSupportedOptionTypes();
}
