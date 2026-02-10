namespace DtPipe.Core.Abstractions;

/// <summary>
/// Interface for mapping CLR types to database/format-specific type strings.
/// Each provider implements this with its own type mapping logic.
/// </summary>
public interface ITypeMapper
{
	/// <summary>
	/// Maps a CLR type to the corresponding provider-specific type string.
	/// </summary>
	/// <param name="clrType">The CLR type to map.</param>
	/// <returns>The provider-specific type string.</returns>
	string MapToProviderType(Type clrType);
}
