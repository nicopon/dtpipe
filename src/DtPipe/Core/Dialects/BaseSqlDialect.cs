using System.Text.RegularExpressions;
using DtPipe.Core.Abstractions;

namespace DtPipe.Core.Dialects;

/// <summary>
/// Base class for SQL dialects implementing common behavior.
/// </summary>
public abstract partial class BaseSqlDialect : ISqlDialect
{
	[GeneratedRegex(@"^[a-zA-Z_][a-zA-Z0-9_]*$")]
	private static partial Regex SimpleIdentifierRegex();

	public abstract string Normalize(string identifier);

	public abstract string Quote(string identifier);

	public virtual bool NeedsQuoting(string identifier)
	{
		if (string.IsNullOrWhiteSpace(identifier)) return false;

		// If it contains non-alphanumeric chars (except underscore), it needs quoting
		if (!SimpleIdentifierRegex().IsMatch(identifier)) return true;

		// Check against reserved keywords (can be overridden by derived classes)
		if (IsReservedKeyword(identifier)) return true;

		// Check case sensitivity requirements of the specific dialect
		if (IsCaseMismatch(identifier)) return true;

		return false;
	}

	protected abstract bool IsReservedKeyword(string identifier);

	/// <summary>
	/// Checks if the identifier's case conflicts with the dialect's default unquoted casing.
	/// </summary>
	protected abstract bool IsCaseMismatch(string identifier);
}
