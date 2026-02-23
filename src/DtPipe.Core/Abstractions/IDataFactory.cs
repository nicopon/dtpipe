namespace DtPipe.Core.Abstractions;

/// <summary>
/// Base factory interface for data components (Readers/Writers) that can be selected via connection string.
/// </summary>
public interface IDataFactory : IComponentDescriptor
{

	/// <summary>
	/// Determines if this factory can handle the given connection string or file path.
	/// Used as a fallback when no prefix is provided.
	/// </summary>
	bool CanHandle(string connectionString);
}
