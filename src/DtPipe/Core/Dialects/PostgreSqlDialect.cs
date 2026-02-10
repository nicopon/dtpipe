namespace DtPipe.Core.Dialects;

public class PostgreSqlDialect : BaseSqlDialect
{
	private static readonly HashSet<string> ReservedKeywords = new(StringComparer.OrdinalIgnoreCase)
	{
		"ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC", "AUTHORIZATION", "BINARY", "BOTH", "CASE", "CAST", "CHECK", "COLLATE", "COLLATION", "COLUMN", "CONCURRENTLY", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO", "ELSE", "END", "EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN", "FREEZE", "FROM", "FULL", "GRANT", "GROUP", "HAVING", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIMESTAMP", "NATURAL", "NOT", "NOTNULL", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVERLAPS", "PLACING", "PRIMARY", "REFERENCES", "RETURNING", "RIGHT", "SELECT", "SESSION_USER", "SIMILAR", "SOME", "SYMMETRIC", "TABLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER", "USING", "VARIADIC", "VERBOSE", "WHEN", "WHERE", "WINDOW", "WITH"
	};

	public override string Normalize(string identifier)
	{
		return identifier.ToLowerInvariant();
	}

	public override string Quote(string identifier)
	{
		return $"\"{identifier}\"";
	}

	protected override bool IsReservedKeyword(string identifier)
	{
		return ReservedKeywords.Contains(identifier);
	}

	protected override bool IsCaseMismatch(string identifier)
	{
		// PostgreSQL normalizes unquoted identifiers to lowercase
		// Quote if contains uppercase to preserve case
		return identifier != identifier.ToLowerInvariant();
	}
}
