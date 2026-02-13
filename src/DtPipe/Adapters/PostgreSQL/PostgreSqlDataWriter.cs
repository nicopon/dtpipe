using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Helpers;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DtPipe.Adapters.PostgreSQL;

public sealed partial class PostgreSqlDataWriter : BaseSqlDataWriter
{
	private readonly PostgreSqlWriterOptions _options;
	private NpgsqlBinaryImporter? _writer;
	private string? _stagingTable;
	private List<string> _keyColumns = new();
	private Type[]? _targetTypes;
	private NpgsqlTypes.NpgsqlDbType[]? _columnTypes;
	private readonly ILogger<PostgreSqlDataWriter> _logger;

	private readonly ISqlDialect _dialect = new DtPipe.Core.Dialects.PostgreSqlDialect();
	public override ISqlDialect Dialect => _dialect;

	public PostgreSqlDataWriter(string connectionString, PostgreSqlWriterOptions options, ILogger<PostgreSqlDataWriter> logger)
		: base(connectionString)
	{
		_options = options;
		_logger = logger;
	}

	protected override IDbConnection CreateConnection(string connectionString)
	{
		return new NpgsqlConnection(connectionString);
	}

	protected override async Task<(string Schema, string Table)> ResolveTargetTableAsync(CancellationToken ct)
	{
		// Use native PG to_regclass to resolve table name (handles search path and existence)
		if (_connection is NpgsqlConnection pgConn)
		{
			var resolved = await ResolveTableNativeAsync(pgConn, _options.Table, ct);
			if (resolved != null)
			{
				return resolved.Value;
			}
		}

		// Fallback for tables that don't exist yet (e.g. Recreate strategy)
		return ParseTableName(_options.Table);
	}

	protected override async Task ApplyWriteStrategyAsync(string resolvedSchema, string resolvedTable, CancellationToken ct)
	{
		if (_options.Strategy == PostgreSqlWriteStrategy.Recreate)
		{
			// Introspect before Drop to preserve native types
			TargetSchemaInfo? existingSchema = null;
			bool tableExists = !string.IsNullOrEmpty(resolvedSchema) || await TableExistsAsync(resolvedTable, ct);

			if (tableExists)
			{
				try
				{
					existingSchema = await InspectTargetAsync(ct);
				}
				catch { /* Ignore introspection errors during recreate */ }

				await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_quotedTargetTableName}", ct);
			}

			string createSql;
			if (existingSchema != null && existingSchema.Exists)
			{
				createSql = BuildCreateTableFromIntrospection(_quotedTargetTableName, existingSchema);
				SyncColumnsFromIntrospection(existingSchema);
			}
			else
			{
				createSql = BuildCreateTableSql(_quotedTargetTableName, _columns!);
			}

			await ExecuteNonQueryAsync(createSql, ct);
		}
		else if (_options.Strategy == PostgreSqlWriteStrategy.DeleteThenInsert)
		{
			await EnsureTableExistsAsync(ct);
			await ExecuteNonQueryAsync($"DELETE FROM {_quotedTargetTableName}", ct);
		}
		else if (_options.Strategy == PostgreSqlWriteStrategy.Truncate)
		{
			await EnsureTableExistsAsync(ct);
			await ExecuteNonQueryAsync($"TRUNCATE TABLE {_quotedTargetTableName}", ct);
		}
		else
		{
			await EnsureTableExistsAsync(ct);
		}

		// PostgreSQL COPY does not support ON CONFLICT. Use staging table for Upsert/Ignore.
		if (_options.Strategy == PostgreSqlWriteStrategy.Upsert || _options.Strategy == PostgreSqlWriteStrategy.Ignore)
		{
			await SetupStagingTableAsync(ct);
		}

		var copyTarget = _stagingTable ?? _quotedTargetTableName;
		var copySql = BuildCopySql(copyTarget, _columns!);

		// Prepare conversion types from target schema
		var targetInfo = await InspectTargetAsync(ct);
		_targetTypes = new Type[_columns!.Count];
		_columnTypes = new NpgsqlTypes.NpgsqlDbType[_columns.Count];

		for (int i = 0; i < _columns.Count; i++)
		{
			var sourceCol = _columns[i];
			var targetCol = targetInfo?.Columns.FirstOrDefault(c => c.Name.Equals(sourceCol.Name, StringComparison.OrdinalIgnoreCase));

			// Use target type if available, otherwise source type
			var effectiveType = targetCol?.InferredClrType ?? sourceCol.ClrType;
			_targetTypes[i] = effectiveType;
			_columnTypes[i] = PostgreSqlTypeMapper.Instance.MapToNpgsqlDbType(effectiveType);
		}

		if (_connection is NpgsqlConnection pgConn)
		{
			_writer = await pgConn.BeginBinaryImportAsync(copySql, ct);
		}
	}


	private async Task EnsureTableExistsAsync(CancellationToken ct)
	{
		// Try CREATE TABLE IF NOT EXISTS
		var createSql = BuildCreateTableSql(_quotedTargetTableName, _columns!);
		await ExecuteNonQueryAsync(createSql, ct);
	}

	private async Task<bool> TableExistsAsync(string tableName, CancellationToken ct)
	{
		if (_connection is NpgsqlConnection pgConn)
		{
			var res = await ResolveTableNativeAsync(pgConn, tableName, ct);
			return res != null;
		}
		return false;
	}

	private async Task SetupStagingTableAsync(CancellationToken ct)
	{
		// 1. Resolve Keys
		var targetInfo = await InspectTargetAsync(ct);
		if (targetInfo?.PrimaryKeyColumns != null)
		{
			_keyColumns.AddRange(targetInfo.PrimaryKeyColumns);
		}

		if (_keyColumns.Count == 0 && !string.IsNullOrEmpty(_options.Key))
		{
			_keyColumns.AddRange(ColumnHelper.ResolveKeyColumns(_options.Key, _columns!));
		}

		if (_keyColumns.Count == 0)
		{
			throw new InvalidOperationException($"Strategy {_options.Strategy} requires a Primary Key. None detected.");
		}

		// 2. Create Staging Table (TEMP table)
		_stagingTable = $"tmp_stage_{Guid.NewGuid():N}";
		await ExecuteNonQueryAsync($"CREATE TEMP TABLE {_stagingTable} (LIKE {_quotedTargetTableName} INCLUDING DEFAULTS)", ct);
	}

	public override async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
	{
		if (_writer is null) throw new InvalidOperationException("Writer not initialized");
		if (_columns is null) throw new InvalidOperationException("Columns not initialized");

		try
		{
			foreach (var row in rows)
			{
				await _writer.StartRowAsync(ct);
				for (int i = 0; i < row.Length; i++)
				{
					var val = row[i];
					if (val is null)
					{
						await _writer.WriteNullAsync(ct);
					}
					else
					{
						// Use strict types and convert value if needed
						var targetType = _targetTypes![i];
						var convertedVal = ValueConverter.ConvertValue(val, targetType);

						// Normalize DateTime for Npgsql 6.0+ strictness based on column type
						if (convertedVal is DateTime dt)
						{
							var dbType = _columnTypes![i];
							if (dbType == NpgsqlTypes.NpgsqlDbType.Timestamp && dt.Kind != DateTimeKind.Unspecified)
							{
								// Writing to 'timestamp' (naive): Convert to Unspecified
								convertedVal = DateTime.SpecifyKind(dt, DateTimeKind.Unspecified);
							}
							else if (dbType == NpgsqlTypes.NpgsqlDbType.TimestampTz && dt.Kind == DateTimeKind.Unspecified)
							{
								// Writing to 'timestamptz': Default to Utc
								convertedVal = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
							}
						}

						await _writer.WriteAsync(convertedVal, _columnTypes![i], ct);
					}
				}
			}
		}
		catch (Exception ex)
		{
			var analysis = await BatchFailureAnalyzer.AnalyzeAsync(this, rows, _columns, ct);
			if (!string.IsNullOrEmpty(analysis))
			{
				throw new InvalidOperationException($"PostgreSQL Binary Import Failed with detailed analysis:\n{analysis}", ex);
			}
			throw;
		}
	}


	public override async ValueTask CompleteAsync(CancellationToken ct = default)
	{
		if (_writer != null)
		{
			await _writer.CompleteAsync(ct);
			await _writer.DisposeAsync();
			_writer = null;

			if (_stagingTable != null)
			{
				await MergeStagingTableAsync(ct);
			}
		}
	}

	private async Task MergeStagingTableAsync(CancellationToken ct)
	{
		var cols = _columns!.Select(c => SqlIdentifierHelper.GetSafeIdentifier(_dialect, c)).ToList();
		var conflictTarget = string.Join(", ", _keyColumns.Select(c => _dialect.Quote(c)));

		var updateSet = string.Join(", ",
			_columns!.Where(c => !_keyColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase))
						.Select(c => $"{SqlIdentifierHelper.GetSafeIdentifier(_dialect, c)} = EXCLUDED.{SqlIdentifierHelper.GetSafeIdentifier(_dialect, c)}"));

		var sb = new StringBuilder();
		string quotedStaging = _dialect.Quote(_stagingTable!);

		sb.Append($"INSERT INTO {_quotedTargetTableName} ({string.Join(", ", cols)}) SELECT {string.Join(", ", cols)} FROM {quotedStaging} ");

		if (_options.Strategy == PostgreSqlWriteStrategy.Ignore)
		{
			sb.Append($"ON CONFLICT ({conflictTarget}) DO NOTHING");
		}
		else if (_options.Strategy == PostgreSqlWriteStrategy.Upsert)
		{
			sb.Append($"ON CONFLICT ({conflictTarget}) DO UPDATE SET {updateSet}");
		}

		await ExecuteNonQueryAsync(sb.ToString(), ct);
		await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_stagingTable}", ct);
	}

	public override async ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
	{
		if (_connection == null)
		{
			_connection = new NpgsqlConnection(_connectionString);
		}

		if (_connection is NpgsqlConnection pgConn)
		{
			if (pgConn.State != ConnectionState.Open) await pgConn.OpenAsync(ct);
			await using var cmd = pgConn.CreateCommand();
			cmd.CommandText = command;
			await cmd.ExecuteNonQueryAsync(ct);
		}
		else
		{
			if (_connection.State != ConnectionState.Open) _connection.Open();
			using var cmd = _connection.CreateCommand();
			cmd.CommandText = command;
			cmd.ExecuteNonQuery();
		}
	}

	protected override ValueTask DisposeResourcesAsync()
	{
		if (_writer != null)
		{
			return _writer.DisposeAsync();
		}
		return ValueTask.CompletedTask;
	}

	#region Helpers (Parsing, Native Resolution, SQL Building)

	protected override string GetCreateTableSql(string tableName, IEnumerable<PipeColumnInfo> columns)
		=> BuildCreateTableSql(tableName, columns.ToList());
	protected override string GetTruncateTableSql(string tableName) => $"TRUNCATE TABLE {tableName}";
	protected override string GetDropTableSql(string tableName) => $"DROP TABLE {tableName}";

	private static async Task<(string Schema, string Table)?> ResolveTableNativeAsync(NpgsqlConnection connection, string inputName, CancellationToken ct)
	{
		var sql = @"
            SELECT n.nspname, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.oid = to_regclass(@input)::oid";

		await using var cmd = connection.CreateCommand();
		cmd.CommandText = sql;
		cmd.Parameters.AddWithValue("input", inputName);

		await using var reader = await cmd.ExecuteReaderAsync(ct);
		if (await reader.ReadAsync(ct))
		{
			return (reader.GetString(0), reader.GetString(1));
		}
		return null;
	}

	private (string Schema, string Table) ParseTableName(string tableName)
	{
		if (string.IsNullOrWhiteSpace(tableName)) return ("", tableName);
		string[] parts = tableName.Split('.');
		if (parts.Length == 2)
		{
			return (NormalizeIdentifier(parts[0]), NormalizeIdentifier(parts[1]));
		}
		return ("", NormalizeIdentifier(tableName));
	}

	private string NormalizeIdentifier(string id) => id.Trim('"');

	private string BuildCreateTableSql(string quotedTableName, IReadOnlyList<PipeColumnInfo> columns)
	{
		var sb = new StringBuilder();
		sb.Append($"CREATE TABLE IF NOT EXISTS {quotedTableName} (");

		for (int i = 0; i < columns.Count; i++)
		{
			if (i > 0) sb.Append(", ");
			var col = columns[i];
			string nameToUse = !col.IsCaseSensitive ? _dialect.Normalize(col.Name) : col.Name;
			string safeName = SqlIdentifierHelper.GetSafeIdentifier(_dialect, nameToUse);
			sb.Append($"{safeName} {PostgreSqlTypeMapper.Instance.MapToProviderType(col.ClrType)}");
		}

		if (!string.IsNullOrEmpty(_options.Key))
		{
			var resolvedKeys = ColumnHelper.ResolveKeyColumns(_options.Key, columns.ToList());
			var safeKeys = resolvedKeys.Select(keyName => SqlIdentifierHelper.GetSafeIdentifier(_dialect, columns.First(c => c.Name == keyName))).ToList();
			sb.Append($", PRIMARY KEY ({string.Join(", ", safeKeys)})");
		}
		sb.Append(")");
		return sb.ToString();
	}

	private string BuildCreateTableFromIntrospection(string quotedTableName, TargetSchemaInfo schemaInfo)
	{
		var sb = new StringBuilder();
		sb.Append($"CREATE TABLE {quotedTableName} (");
		for (int i = 0; i < schemaInfo.Columns.Count; i++)
		{
			if (i > 0) sb.Append(", ");
			var col = schemaInfo.Columns[i];
			sb.Append($"{_dialect.Quote(col.Name)} {col.NativeType}");
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

	private string BuildCopySql(string tableName, IReadOnlyList<PipeColumnInfo> columns)
	{
		var sb = new StringBuilder();
		sb.Append($"COPY {tableName} (");
		for (int i = 0; i < columns.Count; i++)
		{
			if (i > 0) sb.Append(", ");
			sb.Append(SqlIdentifierHelper.GetSafeIdentifier(_dialect, columns[i]));
		}
		sb.Append(") FROM STDIN (FORMAT BINARY)");
		return sb.ToString();
	}

	private static string BuildNativeType(string dataType, string udtName, int? maxLength, int? precision, int? scale)
	{
		return dataType.ToUpperInvariant() switch
		{
			"CHARACTER VARYING" when maxLength.HasValue => $"VARCHAR({maxLength})",
			"CHARACTER VARYING" => "VARCHAR",
			"CHARACTER" when maxLength.HasValue => $"CHAR({maxLength})",
			"CHARACTER" => "CHAR",
			"NUMERIC" when precision.HasValue && scale.HasValue => $"NUMERIC({precision},{scale})",
			"NUMERIC" when precision.HasValue => $"NUMERIC({precision})",
			"NUMERIC" => "NUMERIC",
			_ => udtName.ToUpperInvariant()
		};
	}

	#endregion

	#region ISchemaInspector Implementation

	public override async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
	{
		await using var connection = new NpgsqlConnection(_connectionString);
		await connection.OpenAsync(ct);

		var resolved = await ResolveTableNativeAsync(connection, _options.Table, ct);
		if (resolved == null) return new TargetSchemaInfo([], false, null, null, null);

		var (schemaName, tableName) = resolved.Value;

		var columnsSql = @"
            SELECT column_name, data_type, udt_name, is_nullable, character_maximum_length, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = @schema AND table_name = @table ORDER BY ordinal_position";

		await using var columnsCmd = new NpgsqlCommand(columnsSql, connection);
		columnsCmd.Parameters.AddWithValue("schema", schemaName);
		columnsCmd.Parameters.AddWithValue("table", tableName);

		var pkColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		var pkSql = @"
            SELECT a.attname
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE c.contype = 'p'
              AND c.conrelid = (SELECT oid FROM pg_class WHERE relname = @table AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = @schema))";

		await using (var pkCmd = new NpgsqlCommand(pkSql, connection))
		{
			pkCmd.Parameters.AddWithValue("schema", schemaName);
			pkCmd.Parameters.AddWithValue("table", tableName);
			await using var r = await pkCmd.ExecuteReaderAsync(ct);
			while (await r.ReadAsync(ct)) pkColumns.Add(r.GetString(0));
		}

		var uniqueColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		var uSql = @"
            SELECT a.attname
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE c.contype = 'u'
              AND c.conrelid = (SELECT oid FROM pg_class WHERE relname = @table AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = @schema))";

		await using (var uCmd = new NpgsqlCommand(uSql, connection))
		{
			uCmd.Parameters.AddWithValue("schema", schemaName);
			uCmd.Parameters.AddWithValue("table", tableName);
			await using var r = await uCmd.ExecuteReaderAsync(ct);
			while (await r.ReadAsync(ct)) uniqueColumns.Add(r.GetString(0));
		}

		long? rowCount = null;
		long? sizeBytes = null;
		var sSql = @"
            SELECT
                (SELECT reltuples::bigint FROM pg_class WHERE relname = @table AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = @schema)),
                (SELECT pg_total_relation_size((quote_ident(@schema) || '.' || quote_ident(@table))::regclass))";

		await using (var sCmd = new NpgsqlCommand(sSql, connection))
		{
			sCmd.Parameters.AddWithValue("schema", schemaName);
			sCmd.Parameters.AddWithValue("table", tableName);
			await using var r = await sCmd.ExecuteReaderAsync(ct);
			if (await r.ReadAsync(ct))
			{
				rowCount = r.IsDBNull(0) ? null : r.GetInt64(0);
				sizeBytes = r.IsDBNull(1) ? null : r.GetInt64(1);
			}
		}

		var columns = new List<TargetColumnInfo>();
		await using var colReader = await columnsCmd.ExecuteReaderAsync(ct);
		while (await colReader.ReadAsync(ct))
		{
			var colName = colReader.GetString(0);
			var dataType = colReader.GetString(1);
			var udtName = colReader.GetString(2);
			var isNullable = colReader.GetString(3) == "YES";
			var maxLength = colReader.IsDBNull(4) ? (int?)null : colReader.GetInt32(4);
			var nativeType = BuildNativeType(dataType, udtName, maxLength, colReader.IsDBNull(5) ? null : colReader.GetInt32(5), colReader.IsDBNull(6) ? null : colReader.GetInt32(6));

			columns.Add(new TargetColumnInfo(colName, nativeType, PostgreSqlTypeMapper.Instance.MapFromProviderType(udtName), isNullable, pkColumns.Contains(colName), uniqueColumns.Contains(colName), maxLength, Precision: colReader.IsDBNull(5) ? null : colReader.GetInt32(5), Scale: colReader.IsDBNull(6) ? null : colReader.GetInt32(6), IsCaseSensitive: colName != colName.ToLowerInvariant()));
		}

		return new TargetSchemaInfo(columns, true, rowCount >= 0 ? rowCount : null, sizeBytes, pkColumns.Count > 0 ? pkColumns.ToList() : null, uniqueColumns.Count > 0 ? uniqueColumns.ToList() : null, IsRowCountEstimate: true);
	}
	#endregion

	public override string? GetWriteStrategy() => _options.Strategy.ToString();

	public override IReadOnlyList<string>? GetRequestedPrimaryKeys()
	{
		if (string.IsNullOrEmpty(_options.Key)) return null;
		return _options.Key.Split(',').Select(k => k.Trim()).Where(k => !string.IsNullOrEmpty(k)).ToList();
	}

	public override bool RequiresPrimaryKey() => _options.Strategy is PostgreSqlWriteStrategy.Upsert or PostgreSqlWriteStrategy.Ignore;
}
