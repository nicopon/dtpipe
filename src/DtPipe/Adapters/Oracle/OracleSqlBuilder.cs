using Oracle.ManagedDataAccess.Client;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Helpers;
using System.Text;

namespace DtPipe.Adapters.Oracle;

/// <summary>
/// Builds Oracle-specific SQL statements (MERGE, INSERT) for data writing operations.
/// </summary>
internal static class OracleSqlBuilder
{
    /// <summary>
    /// Builds a MERGE statement SQL and returns parameter types.
    /// </summary>
    public static (string Sql, OracleDbType[] Types) BuildMergeSql(
        string targetTable,
        IReadOnlyList<ColumnInfo> columns,
        List<string> keyColumns,
        ISqlDialect dialect,
        bool isUpsert)
    {
        var sb = new StringBuilder();
        sb.Append($"MERGE INTO {targetTable} T ");
        sb.Append("USING (SELECT ");
        
        for(int i=0; i<columns.Count; i++)
        {
            if (i>0) sb.Append(", ");
            sb.Append($":v{i} AS \"{columns[i].Name}\"");
        }
        sb.Append(" FROM DUAL) S ON (");
        
        for(int i=0; i<keyColumns.Count; i++)
        {
            if (i>0) sb.Append(" AND ");
            var keyName = keyColumns[i];
            var keyCol = columns.First(c => c.Name.Equals(keyName, StringComparison.OrdinalIgnoreCase));
            var safeTKey = SqlIdentifierHelper.GetSafeIdentifier(dialect, keyCol);
            var alias = $"\"{keyCol.Name}\"";
            
            sb.Append($"T.{safeTKey} = S.{alias}");
        }
        sb.Append(") ");
        
        if (isUpsert)
        {
            sb.Append("WHEN MATCHED THEN UPDATE SET ");
            var nonKeys = columns.Where(c => !keyColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase)).ToList();
            for(int i=0; i<nonKeys.Count; i++)
            {
                if (i>0) sb.Append(", ");
                var safeTCol = SqlIdentifierHelper.GetSafeIdentifier(dialect, nonKeys[i]);
                var alias = $"\"{nonKeys[i].Name}\"";
                sb.Append($"T.{safeTCol} = S.{alias}");
            }
        }

        sb.Append(" WHEN NOT MATCHED THEN INSERT (");
        for(int i=0; i<columns.Count; i++)
        {
            if (i>0) sb.Append(", ");
            sb.Append(SqlIdentifierHelper.GetSafeIdentifier(dialect, columns[i]));
        }
        sb.Append(") VALUES (");
        for(int i=0; i<columns.Count; i++)
        {
            if (i>0) sb.Append(", ");
            sb.Append($"S.\"{columns[i].Name}\"");
        }
        sb.Append(")");

        var types = columns.Select(c => OracleTypeMapper.GetOracleDbType(c.ClrType)).ToArray();
        return (sb.ToString(), types);
    }

    /// <summary>
    /// Builds an INSERT statement SQL and returns parameter types.
    /// </summary>
    public static (string Sql, OracleDbType[] Types) BuildInsertSql(
        string targetTable,
        IReadOnlyList<ColumnInfo> columns,
        ISqlDialect dialect,
        bool useAppendHint)
    {
        var sb = new StringBuilder();
        sb.Append("INSERT ");
        
        if (useAppendHint)
        {
            sb.Append("/*+ APPEND */ ");
        }

        sb.Append($"INTO {targetTable} (");
        for (int i = 0; i < columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append(SqlIdentifierHelper.GetSafeIdentifier(dialect, columns[i]));
        }
        sb.Append(") VALUES (");
        for (int i = 0; i < columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($":v{i}");
        }
        sb.Append(")");

        var types = columns.Select(c => OracleTypeMapper.GetOracleDbType(c.ClrType)).ToArray();
        return (sb.ToString(), types);
    }

    /// <summary>
    /// Builds CREATE TABLE DDL from introspection info (preserves native types).
    /// </summary>
    public static string BuildCreateTableFromIntrospection(
        string targetTable,
        TargetSchemaInfo schemaInfo,
        ISqlDialect dialect)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE {targetTable} (");
        
        for (int i = 0; i < schemaInfo.Columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            var col = schemaInfo.Columns[i];
            
            // Quote identifier
            var safeName = dialect.Quote(col.Name);
            
            sb.Append($"{safeName} {col.NativeType}");
            
            if (!col.IsNullable)
            {
                sb.Append(" NOT NULL");
            }
        }
        
        // Add primary key constraint if present
        if (schemaInfo.PrimaryKeyColumns != null && schemaInfo.PrimaryKeyColumns.Count > 0)
        {
            sb.Append(", PRIMARY KEY (");
            for(int i=0; i < schemaInfo.PrimaryKeyColumns.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                sb.Append(dialect.Quote(schemaInfo.PrimaryKeyColumns[i]));
            }
            sb.Append(")");
        }
        
        sb.Append(")");
        return sb.ToString();
    }
}
