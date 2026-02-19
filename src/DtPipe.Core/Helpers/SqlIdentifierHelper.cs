using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Core.Helpers;

/// <summary>
/// Helper methods for SQL identifier quoting and escaping.
/// </summary>
public static class SqlIdentifierHelper
{
	/// <summary>
	/// Gets a safe identifier for a column, quoting if necessary based on case sensitivity or reserved keywords.
	/// </summary>
	public static string GetSafeIdentifier(ISqlDialect dialect, PipeColumnInfo col)
	{
		if (col.IsCaseSensitive || dialect.NeedsQuoting(col.Name))
		{
			return dialect.Quote(col.Name);
		}
		return col.Name;
	}

	/// <summary>
	/// Gets a safe identifier for a column name, quoting if necessary based on reserved keywords.
	/// </summary>
	public static string GetSafeIdentifier(ISqlDialect dialect, string name)
	{
		if (dialect.NeedsQuoting(name))
		{
			return dialect.Quote(name);
		}
		return name;
	}
}
