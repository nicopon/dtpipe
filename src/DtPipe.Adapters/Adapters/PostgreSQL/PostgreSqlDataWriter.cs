using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Helpers;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DtPipe.Adapters.PostgreSQL;

public sealed partial class PostgreSqlDataWriter : BaseSqlDataWriter
{
	private readonly PostgreSqlWriterOptions _options;
	private readonly List<string> _keyColumns = new();
	private Type[]? _targetTypes;
	private string[]? _targetNames;
	private NpgsqlTypes.NpgsqlDbType[]? _columnTypes;
	private readonly ILogger<PostgreSqlDataWriter> _logger;
	private readonly ITypeMapper _typeMapper;
	private bool _metaDataInitialized;

    // Stable mapping: Metadata Index -> Source Index
    private int[]? _targetToSourceMap;

	private readonly ISqlDialect _dialect = new DtPipe.Core.Dialects.PostgreSqlDialect();
	public override ISqlDialect Dialect => _dialect;

	public override bool RequiresTargetInspection => _options.Strategy != PostgreSqlWriteStrategy.Recreate;

	protected override ITypeMapper GetTypeMapper() => _typeMapper;

	public PostgreSqlDataWriter(string connectionString, PostgreSqlWriterOptions options, ILogger<PostgreSqlDataWriter> logger, ITypeMapper typeMapper)
		: base(connectionString)
	{
		_options = options;
		_logger = logger;
		_typeMapper = typeMapper;
	}

	protected override IDbConnection CreateConnection(string connectionString)
	{
		return new NpgsqlConnection(connectionString);
	}

	protected override async Task<(string Schema, string Table)> ResolveTargetTableAsync(CancellationToken ct)
	{
		if (_connection is NpgsqlConnection pgConn)
		{
			var resolved = await ResolveTableNativeAsync(pgConn, _options.Table, ct);
			if (resolved != null)
			{
				return resolved.Value;
			}
		}

		return ParseTableName(_options.Table);
	}

	protected override async Task<TargetSchemaInfo?> ApplyWriteStrategyAsync(string resolvedSchema, string resolvedTable, CancellationToken ct)
	{
		if (_options.Strategy == PostgreSqlWriteStrategy.Recreate)
		{
			TargetSchemaInfo? existingSchema = null;
			try
			{
				existingSchema = await InspectTargetAsync(ct);
			}
			catch { }

			await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_quotedTargetTableName}", ct);

			string createSql;
			if (existingSchema != null && existingSchema.Exists)
			{
				createSql = BuildCreateTableFromIntrospection(_quotedTargetTableName, existingSchema);
				SyncColumnsFromIntrospection(existingSchema);
			}
			else
			{
				createSql = GetCreateTableSql(_quotedTargetTableName, _columns!);
			}

			await ExecuteNonQueryAsync(createSql, ct);
			InvalidateSchemaCache();
			return null;
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

			var previous = await InspectTargetAsync(ct);
			if (previous != null) return previous with { RowCount = 0 };
			InvalidateSchemaCache();
			return null;
		}
		else
		{
			await EnsureTableExistsAsync(ct);
			var exists = await InspectTargetAsync(ct);
			if (exists?.Exists == false)
			{
				InvalidateSchemaCache();
				return null;
			}
		}

		return null;
	}

	private async Task EnsureMetaDataInitializedAsync(CancellationToken ct)
	{
		if (_metaDataInitialized) return;
		await EnsureConnectionOpenAsync(ct);

		if (_options.Strategy == PostgreSqlWriteStrategy.Upsert || _options.Strategy == PostgreSqlWriteStrategy.Ignore)
		{
			await ResolveKeysAsync(ct);
		}

		// Robust Column Mapping
		var targetInfo = await InspectTargetAsync(ct);
		var map = new List<int>();
		var tTypes = new List<Type>();
		var tNames = new List<string>();
		var cTypes = new List<NpgsqlTypes.NpgsqlDbType>();

        if (targetInfo == null || !targetInfo.Exists)
        {
             // Fallback to source columns if target doesn't exist (e.g. Recreate)
             // This keeps the writer stable.
             for(int i=0; i<_columns!.Count; i++) {
                 map.Add(i);
                 tTypes.Add(_columns[i].ClrType);
                 tNames.Add(_columns[i].Name);
                 cTypes.Add(PostgreSqlTypeConverter.Instance.MapToNpgsqlDbType(_columns[i].ClrType));
             }
        }
        else
        {
            for (int i = 0; i < _columns!.Count; i++)
            {
                var sourceCol = _columns[i];
                var targetCol = targetInfo.Columns.FirstOrDefault(c => IsFuzzyMatch(sourceCol.Name, c.Name));

                if (targetCol == null)
                {
                    _logger.LogWarning("Column {ColumnName} not found in target table. Skipping.", sourceCol.Name);
                    continue;
                }

                map.Add(i);
                tTypes.Add(targetCol.InferredClrType ?? sourceCol.ClrType);
                tNames.Add(targetCol.Name);
                cTypes.Add(PostgreSqlTypeConverter.Instance.MapToNpgsqlDbType(targetCol.InferredClrType ?? sourceCol.ClrType));
            }
        }

		_targetToSourceMap = map.ToArray();
		_targetTypes = tTypes.ToArray();
		_targetNames = tNames.ToArray();
		_columnTypes = cTypes.ToArray();

		_metaDataInitialized = true;
	}

	private async Task EnsureTableExistsAsync(CancellationToken ct)
	{
		if (await TableExistsAsync(_quotedTargetTableName, ct))
		{
			return;
		}

		var createSql = GetCreateTableSql(_quotedTargetTableName, _columns!);
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

	private async Task ResolveKeysAsync(CancellationToken ct)
	{
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
	}

	public override async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
	{
		if (rows.Count == 0) return;

		await EnsureConnectionOpenAsync(ct);
		await EnsureMetaDataInitializedAsync(ct);

		try
		{
			if (_options.Strategy is PostgreSqlWriteStrategy.Upsert or PostgreSqlWriteStrategy.Ignore)
			{
				await WriteBatchViaStagingAsync(rows, ct);
			}
			else
			{
				await WriteBatchDirectAsync(rows, ct);
			}
		}
		catch (Exception ex)
		{
			if (_connection != null)
			{
				try { _connection.Dispose(); } catch { }
				_connection = null;
			}

			var analysis = await BatchFailureAnalyzer.AnalyzeAsync(this, rows, _columns!, ct);
			if (!string.IsNullOrEmpty(analysis))
			{
				throw new InvalidOperationException($"PostgreSQL Binary Import Failed with detailed analysis:\n{analysis}", ex);
			}
			throw;
		}
	}

	private async Task WriteBatchDirectAsync(IReadOnlyList<object?[]> rows, CancellationToken ct)
	{
		var copySql = BuildCopySql(_quotedTargetTableName);
        _logger.LogDebug("Executing PostgreSQL Binary Import: {Sql}", copySql);
		await using var writer = await ((NpgsqlConnection)_connection!).BeginBinaryImportAsync(copySql, ct);

		await WriteRowsToCopyAsync(writer, rows, ct);
		await writer.CompleteAsync(ct);
	}

	private async Task WriteBatchViaStagingAsync(IReadOnlyList<object?[]> rows, CancellationToken ct)
	{
		var stagingTable = $"tmp_batch_{Guid.NewGuid():N}";
		await ExecuteNonQueryAsync($"CREATE TEMP TABLE {stagingTable} (LIKE {_quotedTargetTableName} INCLUDING DEFAULTS)", ct);

		try
		{
			var copySql = BuildCopySql(stagingTable);
			await using (var writer = await ((NpgsqlConnection)_connection!).BeginBinaryImportAsync(copySql, ct))
			{
				await WriteRowsToCopyAsync(writer, rows, ct);
				await writer.CompleteAsync(ct);
			}

			await MergeStagingBatchAsync(stagingTable, ct);
		}
		finally
		{
			try
			{
				await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {stagingTable}", ct);
			}
			catch (Exception ex)
			{
				_logger.LogWarning(ex, "Failed to drop staging table {TableName}", stagingTable);
			}
		}
	}

	private async Task WriteRowsToCopyAsync(NpgsqlBinaryImporter writer, IReadOnlyList<object?[]> rows, CancellationToken ct)
	{
		foreach (var row in rows)
		{
			await writer.StartRowAsync(ct);
			for (int j = 0; j < _targetToSourceMap!.Length; j++)
			{
				int sourceIdx = _targetToSourceMap[j];
				var val = row[sourceIdx];
				var targetType = _targetTypes![j];

				if (targetType == typeof(Guid) && val is string s && string.IsNullOrWhiteSpace(s))
				{
					val = null;
				}

				if (val is null || val == DBNull.Value)
				{
					await writer.WriteNullAsync(ct);
				}
				else
				{
					var convertedVal = ValueConverter.ConvertValue(val, targetType);

					if (convertedVal is null || convertedVal == DBNull.Value)
					{
						await writer.WriteNullAsync(ct);
						continue;
					}

					if (convertedVal is DateTime dt)
					{
						var dbType = _columnTypes![j];
						if (dbType == NpgsqlTypes.NpgsqlDbType.Timestamp && dt.Kind != DateTimeKind.Unspecified)
						{
							convertedVal = DateTime.SpecifyKind(dt, DateTimeKind.Unspecified);
						}
						else if (dbType == NpgsqlTypes.NpgsqlDbType.TimestampTz && dt.Kind == DateTimeKind.Unspecified)
						{
							convertedVal = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
						}
					}

					await writer.WriteAsync(convertedVal, _columnTypes![j], ct);
				}
			}
		}
	}

	public override async ValueTask CompleteAsync(CancellationToken ct = default)
	{
		await ValueTask.CompletedTask;
	}

	private async Task MergeStagingBatchAsync(string stagingTable, CancellationToken ct)
	{
		var cols = _targetNames!.Select(n => _dialect.Quote(n)).ToList();
		var conflictTarget = string.Join(", ", _keyColumns.Select(c => _dialect.Quote(c)));
		var quotedStaging = _dialect.Quote(stagingTable);

		var updateSet = string.Join(", ",
			_targetNames!.Where(n => !_keyColumns.Contains(n, StringComparer.OrdinalIgnoreCase))
						.Select(n => $"{_dialect.Quote(n)} = EXCLUDED.{_dialect.Quote(n)}"));

		var sb = new StringBuilder();
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
	}

	public override async ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
	{
		await EnsureConnectionOpenAsync(ct);
		using var cmd = (NpgsqlCommand)_connection!.CreateCommand();
		cmd.CommandText = command;
		await cmd.ExecuteNonQueryAsync(ct);
	}

	protected override ValueTask DisposeResourcesAsync()
	{
		return ValueTask.CompletedTask;
	}

	#region Helpers

	protected override string GetTruncateTableSql(string tableName) => $"TRUNCATE TABLE {tableName}";
	protected override string GetDropTableSql(string tableName) => $"DROP TABLE {tableName}";

	protected override string GetAddColumnSql(string tableName, PipeColumnInfo column)
	{
		var safeName = Dialect.NeedsQuoting(column.Name) ? Dialect.Quote(column.Name) : column.Name;
		var type = _typeMapper.MapToProviderType(column.ClrType);
		var nullability = column.IsNullable ? "" : " NOT NULL";
		return $"ALTER TABLE {tableName} ADD COLUMN {safeName} {type}{nullability}";
	}

	private static async Task<(string Schema, string Table)?> ResolveTableNativeAsync(NpgsqlConnection connection, string inputName, CancellationToken ct)
	{
		var sql = "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = to_regclass(@input)::oid";
		await using var cmd = connection.CreateCommand();
		cmd.CommandText = sql;
		cmd.Parameters.AddWithValue("input", inputName);
		await using var reader = await cmd.ExecuteReaderAsync(ct);
		if (await reader.ReadAsync(ct)) return (reader.GetString(0), reader.GetString(1));
		return null;
	}

	private (string Schema, string Table) ParseTableName(string tableName)
	{
		if (string.IsNullOrWhiteSpace(tableName)) return ("", tableName);
		string[] parts = tableName.Split('.');
		if (parts.Length == 2) return (NormalizeIdentifier(parts[0]), NormalizeIdentifier(parts[1]));
		return ("", NormalizeIdentifier(tableName));
	}

	private string NormalizeIdentifier(string id) => id.Trim('"');

    private static bool IsFuzzyMatch(string name1, string name2)
        => ColumnHelper.IsFuzzyMatch(name1, name2);

	private string BuildCopySql(string tableName)
	{
		var sb = new StringBuilder();
		sb.Append($"COPY {tableName} (");
		for (int i = 0; i < _targetNames!.Length; i++)
		{
			if (i > 0) sb.Append(", ");
			sb.Append(_dialect.Quote(_targetNames[i]));
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

	protected override async Task<TargetSchemaInfo?> InspectTargetInternalAsync(CancellationToken ct = default)
	{
		await using var connection = new NpgsqlConnection(_connectionString);
		await connection.OpenAsync(ct);

        // Use native resolution directly to avoid recursion
		var resolved = await ResolveTableNativeAsync(connection, _options.Table, ct);
		if (resolved == null) return new TargetSchemaInfo([], false, null, null, null);
		var (schemaName, tableName) = resolved.Value;
		var columnsSql = "SELECT column_name, data_type, udt_name, is_nullable, character_maximum_length, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_schema = @schema AND table_name = @table ORDER BY ordinal_position";
		await using var columnsCmd = new NpgsqlCommand(columnsSql, connection);
		columnsCmd.Parameters.AddWithValue("schema", schemaName);
		columnsCmd.Parameters.AddWithValue("table", tableName);
		var pkColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		var pkSql = "SELECT a.attname FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) WHERE c.contype = 'p' AND c.conrelid = (SELECT oid FROM pg_class WHERE relname = @table AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = @schema))";
		await using (var pkCmd = new NpgsqlCommand(pkSql, connection))
		{
			pkCmd.Parameters.AddWithValue("schema", schemaName);
			pkCmd.Parameters.AddWithValue("table", tableName);
			await using var r = await pkCmd.ExecuteReaderAsync(ct);
			while (await r.ReadAsync(ct)) pkColumns.Add(r.GetString(0));
		}
		var uniqueColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		var uSql = "SELECT a.attname FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) WHERE c.contype = 'u' AND c.conrelid = (SELECT oid FROM pg_class WHERE relname = @table AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = @schema))";
		await using (var uCmd = new NpgsqlCommand(uSql, connection))
		{
			uCmd.Parameters.AddWithValue("schema", schemaName);
			uCmd.Parameters.AddWithValue("table", tableName);
			await using var r = await uCmd.ExecuteReaderAsync(ct);
			while (await r.ReadAsync(ct)) uniqueColumns.Add(r.GetString(0));
		}
		long? rowCount = null;
		long? sizeBytes = null;
		var sSql = "SELECT (SELECT reltuples::bigint FROM pg_class WHERE relname = @table AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = @schema)), (SELECT pg_total_relation_size((quote_ident(@schema) || '.' || quote_ident(@table))::regclass))";
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
		await using (var colReader = await columnsCmd.ExecuteReaderAsync(ct))
		{
			while (await colReader.ReadAsync(ct))
			{
				var colName = colReader.GetString(0);
				var dataType = colReader.GetString(1);
				var udtName = colReader.GetString(2);
				var isNullable = colReader.GetString(3) == "YES";
				var maxLength = colReader.IsDBNull(4) ? (int?)null : colReader.GetInt32(4);
				var nativeType = BuildNativeType(dataType, udtName, maxLength, colReader.IsDBNull(5) ? null : colReader.GetInt32(5), colReader.IsDBNull(6) ? null : colReader.GetInt32(6));
				columns.Add(new TargetColumnInfo(colName, nativeType, _typeMapper.MapFromProviderType(udtName), isNullable, pkColumns.Contains(colName), uniqueColumns.Contains(colName), maxLength, Precision: colReader.IsDBNull(5) ? null : colReader.GetInt32(5), Scale: colReader.IsDBNull(6) ? null : colReader.GetInt32(6), IsCaseSensitive: colName != colName.ToLowerInvariant()));
			}
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
