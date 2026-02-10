using System.Data;
using System.Text;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Helpers;
using DtPipe.Core.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.SqlServer;

public class SqlServerDataWriter : BaseSqlDataWriter
{
	private readonly SqlServerWriterOptions _options;
	private SqlBulkCopy? _bulkCopy;
	private DataTable? _bufferTable;
	private bool _isDbCaseSensitive = false;
	private readonly ILogger<SqlServerDataWriter> _logger;

	// Explicit backing field for dialect to implement abstract property
	private readonly ISqlDialect _dialect = new DtPipe.Core.Dialects.SqlServerDialect();
	public override ISqlDialect Dialect => _dialect;
	private Type[]? _targetTypes;
	private string[]? _targetColumnNames;

	public SqlServerDataWriter(string connectionString, SqlServerWriterOptions options, ILogger<SqlServerDataWriter> logger) : base(connectionString)
	{
		_options = options;
		_logger = logger;
	}

	protected override IDbConnection CreateConnection(string connectionString)
	{
		return new SqlConnection(connectionString);
	}


	protected override async Task<(string Schema, string Table)> ResolveTargetTableAsync(CancellationToken ct)
	{
		// 1. Try native resolution using OBJECT_SCHEMA_NAME/OBJECT_NAME
		var resolved = await ResolveTableNativeAsync(_connection as SqlConnection, _options.Table, ct);
		if (resolved != null)
		{
			return resolved.Value;
		}

		// 2. Fallback parsing for non-existent tables
		return ParseTableName(_options.Table);
	}

	private static async Task<(string Schema, string Table)?> ResolveTableNativeAsync(SqlConnection? connection, string inputName, CancellationToken ct)
	{
		if (connection == null) return null;

		var sql = @"
            SELECT
                OBJECT_SCHEMA_NAME(OBJECT_ID(@input)),
                OBJECT_NAME(OBJECT_ID(@input))
            WHERE OBJECT_ID(@input) IS NOT NULL";

		using var cmd = new SqlCommand(sql, connection);
		cmd.CommandTimeout = 600;
		cmd.Parameters.AddWithValue("@input", inputName);

		using var reader = await cmd.ExecuteReaderAsync(ct);
		if (await reader.ReadAsync(ct))
		{
			if (!reader.IsDBNull(0) && !reader.IsDBNull(1))
			{
				return (reader.GetString(0), reader.GetString(1));
			}
		}
		return null;
	}

	private static (string Schema, string Table) ParseTableName(string tableName)
	{
		// Simple fallback parser
		if (string.IsNullOrWhiteSpace(tableName)) return ("dbo", tableName); // dbo is default
		var parts = tableName.Split('.');
		if (parts.Length == 2) return (parts[0].Trim('[', ']'), parts[1].Trim('[', ']'));
		return ("dbo", tableName.Trim('[', ']'));
	}

	protected override async ValueTask OnInitializedAsync(CancellationToken ct)
	{
		await DetectDatabaseCollationAsync(ct);

		var targetInfo = await InspectTargetAsync(ct);
		_bufferTable = new DataTable();

		int colCount = targetInfo?.Columns.Count ?? _columns!.Count;
		_targetTypes = new Type[colCount];
		_targetColumnNames = new string[colCount];

		if (targetInfo != null && targetInfo.Columns.Count > 0)
		{
			for (int i = 0; i < targetInfo.Columns.Count; i++)
			{
				var targetCol = targetInfo.Columns[i];
				_targetColumnNames[i] = targetCol.Name;
				_targetTypes[i] = targetCol.InferredClrType ?? typeof(string);

				var underlying = Nullable.GetUnderlyingType(_targetTypes[i]) ?? _targetTypes[i];
				_bufferTable.Columns.Add(targetCol.Name, underlying);
			}
		}
		else
		{
			// Fallback to source columns
			for (int i = 0; i < _columns!.Count; i++)
			{
				var col = _columns[i];
				_targetColumnNames[i] = col.Name;
				_targetTypes[i] = col.ClrType;
				var underlying = Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType;
				_bufferTable.Columns.Add(col.Name, underlying);
			}
		}

		_bulkCopy = new SqlBulkCopy((SqlConnection)_connection!, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null)
		{
			DestinationTableName = _quotedTargetTableName,
			BatchSize = 0,
			BulkCopyTimeout = 0
		};

		// Map columns by name
		foreach (DataColumn dc in _bufferTable.Columns)
		{
			_bulkCopy.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
		}
	}

	protected override async Task ApplyWriteStrategyAsync(string resolvedSchema, string resolvedTable, CancellationToken ct)
	{

		if (_options.Strategy == SqlServerWriteStrategy.Recreate)
		{
			// Recreate logic is complex (Introspect -> Drop -> Create from Introspection OR Create from Source)
			// We can implement strict logic here.

			// 1. Check if exists (we have _quotedTargetTableName from base now)
			// But we need strict schema/table.

			TargetSchemaInfo? existingSchema = null;
			try
			{
				existingSchema = await InspectTargetAsync(ct);
			}
			catch { /* Ignore */ }

			if (existingSchema?.Exists == true)
			{
				// DROP
				await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_quotedTargetTableName}", ct);

				// CREATE from Introspection
				var createSql = BuildCreateTableFromIntrospection(_quotedTargetTableName, existingSchema);
				await ExecuteNonQueryAsync(createSql, ct);

				// Sync columns metadata from introspection to ensure future DML matches
				// We need to update _columns to match DB casing if we want strictness.
				if (_columns != null)
				{
					var newCols = new List<PipeColumnInfo>(_columns.Count);
					foreach (var col in _columns)
					{
						var introspected = existingSchema.Columns.FirstOrDefault(c => c.Name.Equals(col.Name, StringComparison.OrdinalIgnoreCase));
						if (introspected != null)
						{
							newCols.Add(col with { Name = introspected.Name, IsCaseSensitive = introspected.IsCaseSensitive });
						}
						else
						{
							newCols.Add(col);
						}
					}
					// Updating base protected member
					_columns = newCols;
				}
			}
			else
			{
				// Create from source columns
				var createSql = GetCreateTableSql(_quotedTargetTableName, _columns!);
				await ExecuteNonQueryAsync(createSql, ct);
			}
		}
		else if (_options.Strategy == SqlServerWriteStrategy.Truncate)
		{
			await ExecuteNonQueryAsync(GetTruncateTableSql(_quotedTargetTableName), ct);
		}
		else if (_options.Strategy == SqlServerWriteStrategy.DeleteThenInsert)
		{
			await ExecuteNonQueryAsync($"DELETE FROM {_quotedTargetTableName}", ct);
		}
		else
		{
			// Append/Upsert/Ignore
			// Create if not exists
			// We check existence via InspectTargetAsync or just try create
			var exists = (await InspectTargetAsync(ct))?.Exists ?? false;

			if (!exists)
			{
				var createSql = GetCreateTableSql(_quotedTargetTableName, _columns!);
				try
				{
					await ExecuteNonQueryAsync(createSql, ct);
				}
				catch (SqlException ex) when (ex.Number == 2714) { /* Ignore race */ }
			}
		}

		// Key Analysis for Upsert/Ignore
		if (_options.Strategy == SqlServerWriteStrategy.Upsert || _options.Strategy == SqlServerWriteStrategy.Ignore)
		{
			await AnalyzeKeysAsync(ct);
		}
	}

	private List<string> _keyColumns = new();

	private async Task AnalyzeKeysAsync(CancellationToken ct)
	{
		// 1. Resolve Keys
		var targetInfo = await InspectTargetAsync(ct);
		if (targetInfo?.PrimaryKeyColumns != null)
		{
			_keyColumns.AddRange(targetInfo.PrimaryKeyColumns);
		}

		if (_keyColumns.Count == 0 && !string.IsNullOrEmpty(_options.Key) && _columns != null)
		{
			_keyColumns.AddRange(ColumnHelper.ResolveKeyColumns(_options.Key, _columns));
		}

		if (_keyColumns.Count == 0 && RequiresPrimaryKey())
		{
			throw new InvalidOperationException($"Strategy {_options.Strategy} requires a Primary Key. None detected.");
		}
	}

	public override async ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
	{
		if (_connection == null)
		{
			// Should not happen if initialized, but strictly required by abstraction
			_connection = CreateConnection(_connectionString);
		}

		if (_connection.State != ConnectionState.Open)
		{
			await ((SqlConnection)_connection).OpenAsync(ct);
		}

		using var cmd = new SqlCommand(command, (SqlConnection)_connection);
		cmd.CommandTimeout = 0; // Disable timeout for maintenance commands
		await cmd.ExecuteNonQueryAsync(ct);
	}

	protected override ValueTask DisposeResourcesAsync()
	{
		if (_bulkCopy is IDisposable d) d.Dispose();
		_bufferTable?.Dispose();
		return ValueTask.CompletedTask;
	}

	protected override string GetCreateTableSql(string tableName, IEnumerable<PipeColumnInfo> columns)
	{
		// Use tableName directly as it's already quoted by Base
		var sb = new StringBuilder();
		sb.Append($"CREATE TABLE {tableName} (");

		var cols = new List<string>();
		foreach (var col in columns)
		{
			string type = SqlServerTypeMapper.MapToProviderType(col.ClrType);
			cols.Add($"{SqlIdentifierHelper.GetSafeIdentifier(_dialect, col)} {type} NULL");
		}
		sb.Append(string.Join(", ", cols));
		sb.Append(")");
		return sb.ToString();
	}

	protected override string GetTruncateTableSql(string tableName) => $"TRUNCATE TABLE {tableName}";
	protected override string GetDropTableSql(string tableName) => $"DROP TABLE {tableName}";

	public override async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
	{
		if (_bulkCopy == null || _bufferTable == null || _columns == null) throw new InvalidOperationException("Not initialized");

		if (_options.Strategy == SqlServerWriteStrategy.Upsert || _options.Strategy == SqlServerWriteStrategy.Ignore)
		{
			await ExecuteMergeAsync(rows, ct);
			return;
		}

		await ExecuteBulkInsertAsync(rows, ct);
	}

	private async Task ExecuteBulkInsertAsync(IEnumerable<object?[]> rows, CancellationToken ct)
	{
		_bufferTable!.Clear();
		foreach (var row in rows)
		{
			var dataRow = _bufferTable.NewRow();
			for (int i = 0; i < _bufferTable.Columns.Count; i++)
			{
				var colName = _bufferTable.Columns[i].ColumnName;
				var targetType = _targetTypes![i];

				// Find source index for this target column
				int sourceIndex = -1;
				for (int s = 0; s < _columns!.Count; s++)
				{
					if (string.Equals(_columns[s].Name, colName, StringComparison.OrdinalIgnoreCase))
					{
						sourceIndex = s;
						break;
					}
				}

				if (sourceIndex != -1)
				{
					var val = row[sourceIndex];
					dataRow[i] = (val == null || val == DBNull.Value) ? DBNull.Value : ConvertValue(val, targetType);
				}
				else
				{
					dataRow[i] = DBNull.Value;
				}
			}
			_bufferTable.Rows.Add(dataRow);
		}

		await _bulkCopy!.WriteToServerAsync(_bufferTable, ct);
	}

	private async Task ExecuteMergeAsync(IReadOnlyList<object?[]> rows, CancellationToken ct)
	{
		// 1. Create Staging Table for Merge
		// utilizing a local temporary table (#Stage_...) ensuring isolation
		var stageTable = $"#Stage_{Guid.NewGuid():N}";

		// Select into strict syntax to create empty staging table structure
		await ExecuteNonQueryAsync($"SELECT TOP 0 * INTO [{stageTable}] FROM {_quotedTargetTableName}", ct);

		try
		{
			// 2. Bulk Copy to Stage
			using var stageBulk = new SqlBulkCopy((SqlConnection)_connection!, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null)
			{
				DestinationTableName = stageTable,
				BatchSize = 0,
				BulkCopyTimeout = 0
			};

			foreach (DataColumn dc in _bufferTable!.Columns)
			{
				stageBulk.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
			}

			_bufferTable!.Clear();
			foreach (var row in rows)
			{
				var dataRow = _bufferTable.NewRow();
				for (int i = 0; i < _bufferTable.Columns.Count; i++)
				{
					var colName = _bufferTable.Columns[i].ColumnName;
					var targetType = _targetTypes![i];

					int sourceIndex = -1;
					for (int s = 0; s < _columns!.Count; s++)
					{
						if (string.Equals(_columns[s].Name, colName, StringComparison.OrdinalIgnoreCase))
						{
							sourceIndex = s;
							break;
						}
					}

					if (sourceIndex != -1)
					{
						var val = row[sourceIndex];
						dataRow[i] = (val == null || val == DBNull.Value) ? DBNull.Value : ConvertValue(val, targetType);
					}
					else
					{
						dataRow[i] = DBNull.Value;
					}
				}
				_bufferTable.Rows.Add(dataRow);
			}
			await stageBulk.WriteToServerAsync(_bufferTable, ct);

			// 3. Perform Merge
			var sb = new StringBuilder();
			sb.Append($"MERGE {_quotedTargetTableName} AS T ");
			sb.Append($"USING [{stageTable}] AS S ON (");

			for (int i = 0; i < _keyColumns.Count; i++)
			{
				if (i > 0) sb.Append(" AND ");
				var keyCol = _columns!.FirstOrDefault(c => c.Name.Equals(_keyColumns[i], StringComparison.OrdinalIgnoreCase));
				var safeKey = keyCol != null ? SqlIdentifierHelper.GetSafeIdentifier(_dialect, keyCol) : _dialect.Quote(_keyColumns[i]);
				sb.Append($"T.{safeKey} = S.[{_keyColumns[i]}]");
			}
			sb.Append(") ");

			if (_options.Strategy == SqlServerWriteStrategy.Upsert)
			{
				sb.Append("WHEN MATCHED THEN UPDATE SET ");
				var nonKeys = _columns!.Where(c => !_keyColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase)).ToList();
				for (int i = 0; i < nonKeys.Count; i++)
				{
					if (i > 0) sb.Append(", ");
					var safeName = SqlIdentifierHelper.GetSafeIdentifier(_dialect, nonKeys[i]);
					sb.Append($"T.{safeName} = S.[{nonKeys[i].Name}]");
				}
			}

			sb.Append(" WHEN NOT MATCHED THEN INSERT (");
			for (int i = 0; i < _columns!.Count; i++)
			{
				if (i > 0) sb.Append(", ");
				sb.Append(SqlIdentifierHelper.GetSafeIdentifier(_dialect, _columns[i]));
			}
			sb.Append(") VALUES (");
			for (int i = 0; i < _columns.Count; i++)
			{
				if (i > 0) sb.Append(", ");
				sb.Append($"S.[{_columns[i].Name}]");
			}
			sb.Append(");");

			await ExecuteNonQueryAsync(sb.ToString(), ct);
		}
		finally
		{
			await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS [{stageTable}]", ct);
		}
	}

	private object ConvertValue(object val, Type targetType)
	{
		if (val == null || val == DBNull.Value) return DBNull.Value;
		var underlyingTarget = Nullable.GetUnderlyingType(targetType) ?? targetType;
		if (underlyingTarget.IsInstanceOfType(val)) return val;

		if (val is string s)
		{
			if (underlyingTarget == typeof(Guid)) return Guid.Parse(s);
			if (underlyingTarget == typeof(DateTime)) return DateTime.Parse(s);
			if (underlyingTarget == typeof(DateTimeOffset)) return DateTimeOffset.Parse(s);
			if (underlyingTarget == typeof(bool)) return bool.Parse(s);
			return Convert.ChangeType(s, underlyingTarget, System.Globalization.CultureInfo.InvariantCulture);
		}

		return Convert.ChangeType(val, underlyingTarget, System.Globalization.CultureInfo.InvariantCulture);
	}

	#region Helpers (Introspection & Collation) - Kept from original

	private async Task DetectDatabaseCollationAsync(CancellationToken ct)
	{
		if (_connection == null) return;
		try
		{
			var dbCmd = _connection.CreateCommand();
			dbCmd.CommandTimeout = 600;
			dbCmd.CommandText = "SELECT collation_name FROM sys.databases WHERE name = DB_NAME()";
			if (dbCmd is SqlCommand sqlCmd)
			{
				var result = await sqlCmd.ExecuteScalarAsync(ct);
				if (result != null && result != DBNull.Value)
				{
					var collation = (string)result;
					_isDbCaseSensitive = collation.Contains("_CS", StringComparison.OrdinalIgnoreCase);
				}
			}
		}
		catch { _isDbCaseSensitive = false; }
	}

	// ISchemaInspector ... (Kept similar to original but using _connection from base)
	public override async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
	{
		// Create NEW connection for inspection to avoid interference with main connection state/transaction if any
		await using var connection = new SqlConnection(_connectionString);
		await connection.OpenAsync(ct);

		// Resolve target using native logic
		var resolved = await ResolveTableNativeAsync(connection, _options.Table, ct);
		if (resolved == null) return new TargetSchemaInfo([], false, null, null, null);

		var (schema, table) = resolved.Value;

		await DetectDatabaseCollationAsync(ct); // Update local state for CS check

		// ... exact same introspection logic as before ...
		// Re-implementing logic here for brevity in diff but strictly copying the logic

		// 2. Get Columns
		var cols = new List<TargetColumnInfo>();
		{
			var colCmd = new SqlCommand(@"
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLLATION_NAME, DATETIME_PRECISION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = @Table
                ORDER BY ORDINAL_POSITION", connection);
			colCmd.CommandTimeout = 600;
			colCmd.Parameters.AddWithValue("@Schema", schema);
			colCmd.Parameters.AddWithValue("@Table", table);

			using var reader = await colCmd.ExecuteReaderAsync(ct);
			while (await reader.ReadAsync(ct))
			{
				var name = reader.GetString(0);
				var type = reader.GetString(1);
				var nullable = reader.GetString(2) == "YES";
				var maxLength = reader.IsDBNull(3) ? null : (int?)Convert.ToInt32(reader[3]);
				var precision = reader.IsDBNull(4) ? null : (int?)Convert.ToInt32(reader[4]);
				var scale = reader.IsDBNull(5) ? null : (int?)Convert.ToInt32(reader[5]);
				var collation = reader.IsDBNull(6) ? null : reader.GetString(6);
				var datetimePrecision = reader.IsDBNull(7) ? null : (int?)Convert.ToInt32(reader[7]);
				var finalPrecision = precision ?? datetimePrecision;
				bool isColCaseSensitive = collation != null
					? collation.Contains("_CS", StringComparison.OrdinalIgnoreCase)
					: _isDbCaseSensitive;

				cols.Add(new TargetColumnInfo(
					name, type, SqlServerTypeMapper.MapFromProviderType(type), nullable, false, false, maxLength, finalPrecision, scale, IsCaseSensitive: isColCaseSensitive
				));
			}
		}

		// Get Primary Key
		var pkCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		{
			var pkCmd = new SqlCommand(@"
                SELECT KCU.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU ON TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
                    AND TC.TABLE_SCHEMA = KCU.TABLE_SCHEMA
                WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
                  AND TC.TABLE_SCHEMA = @Schema
                  AND TC.TABLE_NAME = @Table
                ORDER BY KCU.ORDINAL_POSITION", connection);
			pkCmd.CommandTimeout = 600;
			pkCmd.Parameters.AddWithValue("@Schema", schema);
			pkCmd.Parameters.AddWithValue("@Table", table);
			using var pkReader = await pkCmd.ExecuteReaderAsync(ct);
			while (await pkReader.ReadAsync(ct)) pkCols.Add(pkReader.GetString(0));
		}

		// Get Unique Columns
		var uniqueCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		{
			var uqCmd = new SqlCommand(@"
                SELECT KCU.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU ON TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
                    AND TC.TABLE_SCHEMA = KCU.TABLE_SCHEMA
                WHERE TC.CONSTRAINT_TYPE = 'UNIQUE'
                  AND TC.TABLE_SCHEMA = @Schema
                  AND TC.TABLE_NAME = @Table
                ORDER BY KCU.ORDINAL_POSITION", connection);
			uqCmd.CommandTimeout = 600;
			uqCmd.Parameters.AddWithValue("@Schema", schema);
			uqCmd.Parameters.AddWithValue("@Table", table);
			using var uqReader = await uqCmd.ExecuteReaderAsync(ct);
			while (await uqReader.ReadAsync(ct)) uniqueCols.Add(uqReader.GetString(0));
		}

		// Get Row Count Estimate
		var countCmd = new SqlCommand(@"
            SELECT SUM(p.rows)
            FROM sys.partitions p
            JOIN sys.tables t ON p.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = @Table AND s.name = @Schema AND p.index_id < 2", connection);
		countCmd.CommandTimeout = 600;
		countCmd.Parameters.AddWithValue("@Schema", schema);
		countCmd.Parameters.AddWithValue("@Table", table);
		var countResult = await countCmd.ExecuteScalarAsync(ct);
		long? rowCount = countResult != null && countResult != DBNull.Value ? Convert.ToInt64(countResult) : null;

		return new TargetSchemaInfo(cols, true, rowCount, null, pkCols.Count > 0 ? pkCols.ToList() : null, uniqueCols.Count > 0 ? uniqueCols.ToList() : null, IsRowCountEstimate: true);
	}

	private string BuildCreateTableFromIntrospection(string quotedTableName, TargetSchemaInfo schemaInfo)
	{
		var sb = new StringBuilder();
		sb.Append($"CREATE TABLE {quotedTableName} (");

		for (int i = 0; i < schemaInfo.Columns.Count; i++)
		{
			if (i > 0) sb.Append(", ");
			var col = schemaInfo.Columns[i];
			var safeName = col.IsCaseSensitive || _dialect.NeedsQuoting(col.Name) ? _dialect.Quote(col.Name) : col.Name;
			var typeLower = col.NativeType.ToLowerInvariant();
			var fullType = col.NativeType;

			if (col.MaxLength.HasValue && (typeLower.Contains("char") || typeLower.Contains("binary")))
			{
				var lenStr = col.MaxLength.Value == -1 ? "MAX" : col.MaxLength.Value.ToString();
				fullType += $"({lenStr})";
			}
			else if (col.Precision.HasValue && col.Scale.HasValue && (typeLower == "decimal" || typeLower == "numeric"))
			{
				fullType += $"({col.Precision.Value},{col.Scale.Value})";
			}
			else if (col.Precision.HasValue && (typeLower.Contains("datetime2") || typeLower.Contains("datetimeoffset") || typeLower.Contains("time")))
			{
				fullType += $"({col.Precision.Value})";
			}

			sb.Append($"{safeName} {fullType}");
			if (!col.IsNullable) sb.Append(" NOT NULL");
		}

		if (schemaInfo.PrimaryKeyColumns != null && schemaInfo.PrimaryKeyColumns.Count > 0)
		{
			sb.Append(", PRIMARY KEY (");
			for (int i = 0; i < schemaInfo.PrimaryKeyColumns.Count; i++)
			{
				if (i > 0) sb.Append(", ");
				sb.Append(_dialect.Quote(schemaInfo.PrimaryKeyColumns[i]));
			}
			sb.Append(")");
		}
		sb.Append(")");
		return sb.ToString();
	}
	#endregion

	// IKeyValidator methods
	public override string? GetWriteStrategy() => _options.Strategy.ToString();
	public override IReadOnlyList<string>? GetRequestedPrimaryKeys()
	{
		if (string.IsNullOrEmpty(_options.Key)) return null;
		return _options.Key.Split(',').Select(k => k.Trim()).Where(k => !string.IsNullOrEmpty(k)).ToList();
	}
	public override bool RequiresPrimaryKey() => _options.Strategy is SqlServerWriteStrategy.Upsert or SqlServerWriteStrategy.Ignore;
}
