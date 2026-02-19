namespace DtPipe.Core.Dialects;

public class OracleDialect : BaseSqlDialect
{
	public static readonly OracleDialect Instance = new();
	private static readonly HashSet<string> ReservedKeywords = new(StringComparer.OrdinalIgnoreCase)
	{
		"ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUDIT", "BETWEEN", "BY", "CHAR", "CHECK", "CLUSTER", "COLUMN", "COMMENT", "COMPRESS", "CONNECT", "CREATE", "CURRENT", "DATE", "DECIMAL", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE", "EXCLUSIVE", "EXISTS", "FILE", "FLOAT", "FOR", "FROM", "GRANT", "GROUP", "HAVING", "IDENTIFIED", "IMMEDIATE", "IN", "INCREMENT", "INDEX", "INITIAL", "INSERT", "INTEGER", "INTERSECT", "INTO", "IS", "LEVEL", "LIKE", "LOCK", "LONG", "MAXEXTENTS", "MINUS", "MODE", "MODIFY", "NOAUDIT", "NOCOMPRESS", "NOT", "NOWAIT", "NULL", "NUMBER", "OF", "OFFLINE", "ON", "ONLINE", "OPTION", "OR", "ORDER", "PCTFREE", "PRIOR", "PRIVILEGES", "PUBLIC", "RAW", "RENAME", "RESOURCE", "REVOKE", "ROW", "ROWID", "ROWNUM", "ROWS", "SELECT", "SESSION", "SET", "SHARE", "SIZE", "SMALLINT", "START", "SUCCESSFUL", "SYNONYM", "SYSDATE", "TABLE", "THEN", "TO", "TRIGGER", "UID", "UNION", "UNIQUE", "UPDATE", "USER", "VALIDATE", "VALUES", "VARCHAR", "VARCHAR2", "VIEW", "WHENEVER", "WHERE", "WITH"
	};

	public override string Normalize(string identifier)
	{
		return identifier.ToUpperInvariant();
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
		// Oracle normalizes unquoted identifiers to UPPERCASE
		// Quote if contains lowercase to preserve case
		return identifier != identifier.ToUpperInvariant();
	}
}
