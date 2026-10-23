using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.Shared.Dialects;

public class SqlServerDialect : BaseSqlDialect
{
    /// <summary>SQL Server has no true equivalent: ApplicationIntent=ReadOnly only routes to an availability-group replica and does not make a session read-only. Returning null is the honest answer, and the sample report tells the user it fell back to the verb scan.</summary>
    public override string? ReadOnlySessionSql => null;

    private static readonly HashSet<string> ReservedKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "BACKUP", "BEGIN", "BETWEEN", "BREAK", "BROWSE", "BULK", "BY", "CASCADE", "CASE", "CHECK", "CHECKPOINT", "CLOSE", "CLUSTERED", "COALESCE", "COLLATE", "COLUMN", "COMMIT", "COMPUTE", "CONSTRAINT", "CONTAINS", "CONTAINSTABLE", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DBCC", "DEALLOCATE", "DECLARE", "DEFAULT", "DELETE", "DENY", "DESC", "DISK", "DISTINCT", "DISTRIBUTED", "DOUBLE", "DROP", "DUMP", "ELSE", "END", "ERRLVL", "ESCAPE", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXTERNAL", "FETCH", "FILE", "FILLFACTOR", "FOR", "FOREIGN", "FREETEXT", "FREETEXTTABLE", "FROM", "FULL", "FUNCTION", "GOTO", "GRANT", "GROUP", "HAVING", "HOLDLOCK", "IDENTITY", "IDENTITY_INSERT", "IDENTITYCOL", "IF", "IN", "INDEX", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "KEY", "KILL", "LEFT", "LIKE", "LINENO", "LOAD", "MERGE", "NATIONAL", "NOCHECK", "NONCLUSTERED", "NOT", "NULL", "NULLIF", "OF", "OFF", "OFFSETS", "ON", "OPEN", "OPENDATASOURCE", "OPENQUERY", "OPENROWSET", "OPENXML", "OPTION", "OR", "ORDER", "OUTER", "OVER", "PERCENT", "PIVOT", "PLAN", "PRECISION", "PRIMARY", "PRINT", "PROC", "PROCEDURE", "PUBLIC", "RAISERROR", "READ", "READTEXT", "RECONFIGURE", "REFERENCES", "REPLICATION", "RESTORE", "RESTRICT", "RETURN", "REVERT", "REVOKE", "RIGHT", "ROLLBACK", "ROWCOUNT", "ROWGUIDCOL", "RULE", "SAVE", "SCHEMA", "SECURITYAUDIT", "SELECT", "SEMANTICKEYPHRASETABLE", "SEMANTICSIMILARITYDETAILSTABLE", "SEMANTICSIMILARITYTABLE", "SESSION_USER", "SET", "SETUSER", "SHUTDOWN", "SOME", "STATISTICS", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "TEXTSIZE", "THEN", "TO", "TOP", "TRAN", "TRANSACTION", "TRIGGER", "TRUNCATE", "TRY_CONVERT", "TSEQUAL", "UNION", "UNIQUE", "UNPIVOT", "UPDATE", "UPDATETEXT", "USE", "USER", "VALUES", "VARYING", "VIEW", "WAITFOR", "WHEN", "WHERE", "WHILE", "WITH", "WITHIN GROUP", "WRITETEXT"
    };

    public override string Normalize(string identifier)
    {
        return identifier;
    }

    public override string Quote(string identifier)
    {
        return $"[{identifier}]";
    }

    protected override bool IsReservedKeyword(string identifier)
    {
        return ReservedKeywords.Contains(identifier);
    }

    protected override bool IsCaseMismatch(string identifier)
    {
        return false;
    }

    public override string? TableDiscoveryQuery => "SELECT TABLE_NAME AS table_name, TABLE_TYPE AS table_type FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME";

    // F9 — TSQL MERGE over a staging table (ported from SqlServerDataWriter).
    // Ignore mode omits the WHEN MATCHED clause: existing keys are left untouched,
    // only new keys are inserted (same semantics as ON CONFLICT DO NOTHING).
    public override string BuildStagingMerge(MergeSpec spec)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append($"MERGE {spec.QuotedTargetTable} AS T ");
        sb.Append($"USING [{spec.SourceTable}] AS S ON (");

        for (int i = 0; i < spec.KeyColumns.Count; i++)
        {
            if (i > 0) sb.Append(" AND ");
            var keyCol = spec.Columns.FirstOrDefault(c => c.Name.Equals(spec.KeyColumns[i], StringComparison.OrdinalIgnoreCase));
            var safeKey = keyCol != null ? DtPipe.Core.Helpers.SqlIdentifierHelper.GetSafeIdentifier(this, keyCol) : Quote(spec.KeyColumns[i]);
            sb.Append($"T.{safeKey} = S.[{spec.KeyColumns[i]}]");
        }
        sb.Append(") ");

        if (spec.Mode == MergeMode.Upsert)
        {
            sb.Append("WHEN MATCHED THEN UPDATE SET ");
            var nonKeys = spec.Columns.Where(c => !spec.KeyColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase)).ToList();
            for (int i = 0; i < nonKeys.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                sb.Append($"T.{DtPipe.Core.Helpers.SqlIdentifierHelper.GetSafeIdentifier(this, nonKeys[i])} = S.[{nonKeys[i].Name}]");
            }
        }

        sb.Append(" WHEN NOT MATCHED THEN INSERT (");
        for (int i = 0; i < spec.Columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append(DtPipe.Core.Helpers.SqlIdentifierHelper.GetSafeIdentifier(this, spec.Columns[i]));
        }
        sb.Append(") VALUES (");
        for (int i = 0; i < spec.Columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"S.[{spec.Columns[i].Name}]");
        }
        sb.Append(");");
        return sb.ToString();
    }
}
