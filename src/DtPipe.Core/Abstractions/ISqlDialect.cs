namespace DtPipe.Core.Abstractions;

/// <summary>
/// Defines dialect-specific behaviors for SQL generation, particularly regarding identifier casing and quoting.
/// </summary>
public interface ISqlDialect
{
	/// <summary>
	/// Normalizes an identifier according to the database's default casing rules.
	/// e.g. "MyTable" -> "mytable" (Postgres), "MYTABLE" (Oracle), "MyTable" (SQL Server/SQLite).
	/// </summary>
	string Normalize(string identifier);

	/// <summary>
	/// Quotes an identifier to preserve case and handle special characters.
	/// e.g. "MyTable" -> "\"MyTable\"" (Postgres/Oracle), "[MyTable]" (SQL Server).
	/// </summary>
	string Quote(string identifier);

	/// <summary>
	/// Determines whether an identifier needs quoting based on the dialect's rules and the input string.
	/// Use this to implement "Smart Quoting".
	/// </summary>
	bool NeedsQuoting(string identifier);
}
