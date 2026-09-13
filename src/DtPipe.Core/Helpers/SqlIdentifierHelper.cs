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

	/// <summary>
	/// Quotes a possibly qualified table name — <c>schema.table</c>, or <c>db.schema.table</c> —
	/// one segment at a time, with the dialect's own quoting characters.
	/// </summary>
	/// <remarks>
	/// Quoting the whole value as a single identifier is what made a source-side
	/// <c>--table eshop.customers</c> look for a relation literally named "eshop.customers",
	/// while every writer split the same value on the dot. It also emitted double quotes for
	/// MySQL, which only accepts them under ANSI_QUOTES.
	///
	/// A value already carrying a quote character is returned untouched: the caller has spelled
	/// out what it means, including any dot that belongs inside a name rather than between two.
	/// A null dialect falls back to double quotes, the SQL standard form.
	/// </remarks>
	public static string QuoteQualifiedName(ISqlDialect? dialect, string name)
	{
		if (string.IsNullOrWhiteSpace(name)) return name;
		if (name.IndexOfAny(QuoteCharacters) >= 0) return name;

		var quote = dialect is null ? (Func<string, string>)(part => $"\"{part}\"") : dialect.Quote;
		return string.Join(".", name.Split('.').Select(quote));
	}

	private static readonly char[] QuoteCharacters = { '"', '`', '[', ']' };
}
