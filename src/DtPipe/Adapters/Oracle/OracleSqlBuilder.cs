using System.Text;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Helpers;
using DtPipe.Core.Models;
using Oracle.ManagedDataAccess.Client;

namespace DtPipe.Adapters.Oracle;

internal static class OracleSqlBuilder
{
	public static (string Sql, OracleDbType[] Types) BuildMergeSql(
		string targetTable,
		IReadOnlyList<PipeColumnInfo> columns,
		List<string> keyColumns,
		ISqlDialect dialect,
		bool isUpsert,
		OracleDateTimeMapping dateTimeMapping)
	{
		var sb = new StringBuilder();
		sb.Append($"MERGE INTO {targetTable} T ");
		sb.Append("USING (SELECT ");

		for (int i = 0; i < columns.Count; i++)
		{
			if (i > 0) sb.Append(", ");
			sb.Append($":v{i} AS \"{columns[i].Name}\"");
		}
		sb.Append(" FROM DUAL) S ON (");

		for (int i = 0; i < keyColumns.Count; i++)
		{
			if (i > 0) sb.Append(" AND ");
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
			for (int i = 0; i < nonKeys.Count; i++)
			{
				if (i > 0) sb.Append(", ");
				var safeTCol = SqlIdentifierHelper.GetSafeIdentifier(dialect, nonKeys[i]);
				var alias = $"\"{nonKeys[i].Name}\"";
				sb.Append($"T.{safeTCol} = S.{alias}");
			}
		}

		sb.Append(" WHEN NOT MATCHED THEN INSERT (");
		for (int i = 0; i < columns.Count; i++)
		{
			if (i > 0) sb.Append(", ");
			sb.Append(SqlIdentifierHelper.GetSafeIdentifier(dialect, columns[i]));
		}
		sb.Append(") VALUES (");
		for (int i = 0; i < columns.Count; i++)
		{
			if (i > 0) sb.Append(", ");
			sb.Append($"S.\"{columns[i].Name}\"");
		}
		sb.Append(')');

		var types = columns.Select(c => OracleTypeMapper.GetOracleDbType(c.ClrType, dateTimeMapping)).ToArray();
		return (sb.ToString(), types);
	}

	public static (string Sql, OracleDbType[] Types) BuildInsertSql(
		string targetTable,
		IReadOnlyList<PipeColumnInfo> columns,
		ISqlDialect dialect,
		bool useAppendHint,
		OracleDateTimeMapping dateTimeMapping)
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
		sb.Append(')');

		var types = columns.Select(c => OracleTypeMapper.GetOracleDbType(c.ClrType, dateTimeMapping)).ToArray();
		return (sb.ToString(), types);
	}

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

			var safeName = col.IsCaseSensitive || dialect.NeedsQuoting(col.Name) ? dialect.Quote(col.Name) : col.Name;

			sb.Append($"{safeName} {col.NativeType}");

			if (!col.IsNullable)
			{
				sb.Append(" NOT NULL");
			}
		}

		if (schemaInfo.PrimaryKeyColumns != null && schemaInfo.PrimaryKeyColumns.Count > 0)
		{
			sb.Append(", PRIMARY KEY (");
			for (int i = 0; i < schemaInfo.PrimaryKeyColumns.Count; i++)
			{
				if (i > 0) sb.Append(", ");
				sb.Append(dialect.Quote(schemaInfo.PrimaryKeyColumns[i]));
			}
			sb.Append(')');
		}

		sb.Append(')');
		return sb.ToString();
	}
}
