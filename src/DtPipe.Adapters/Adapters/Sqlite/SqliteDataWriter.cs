using System.Data;
using System.Data.Common;
using System.Text;
using System.Text.RegularExpressions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Helpers;
using DtPipe.Core.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Sqlite;

public sealed class SqliteDataWriter : BaseSqlDataWriter
{
	private readonly SqliteWriterOptions _options;
	private readonly ILogger<SqliteDataWriter> _logger;
	private readonly ITypeMapper _typeMapper;
	private List<string> _keyColumns = new();

	private readonly ISqlDialect _dialect = new DtPipe.Core.Dialects.SqliteDialect();
	public override ISqlDialect Dialect => _dialect;

	protected override ITypeMapper GetTypeMapper() => _typeMapper;

	public SqliteDataWriter(string connectionString, SqliteWriterOptions options, ILogger<SqliteDataWriter> logger, ITypeMapper typeMapper) : base(connectionString)
	{
		_options = options;
		_logger = logger;
		_typeMapper = typeMapper;
	}

	protected override IDbConnection CreateConnection(string connectionString)
	{
		return new SqliteConnection(connectionString);
	}

	protected override Task<(string Schema, string Table)> ResolveTargetTableAsync(CancellationToken ct)
	{
		// SQLite doesn't really use schemas in the same way (usually just 'main')
		return Task.FromResult((string.Empty, _options.Table));
	}

	protected override async Task ApplyWriteStrategyAsync(string resolvedSchema, string resolvedTable, CancellationToken ct)
	{
		if (_options.Strategy == SqliteWriteStrategy.Recreate)
		{
			// Recreate Strategy:
			// 1. Introspect existing table (if any) to preserve schema types
			TargetSchemaInfo? existingSchema = null;
			try { existingSchema = await InspectTargetAsync(ct); } catch { }

			// Drop
			await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_quotedTargetTableName}", ct);
			if (_logger.IsEnabled(LogLevel.Information)) _logger.LogInformation("Dropped table {Table}", _quotedTargetTableName);

			// Recreate
			if (existingSchema?.Exists == true && existingSchema.Columns.Count > 0)
			{
				var recreateSql = BuildCreateTableFromIntrospection(_quotedTargetTableName, existingSchema);
				await ExecuteNonQueryAsync(recreateSql, ct);
			}
			else
			{
				var createTableSql = GetCreateTableSql(_quotedTargetTableName, _columns!);
				await ExecuteNonQueryAsync(createTableSql, ct);
			}
			if (_logger.IsEnabled(LogLevel.Information)) _logger.LogInformation("Created table {Table}", _quotedTargetTableName);
		}
		else if (_options.Strategy == SqliteWriteStrategy.DeleteThenInsert)
		{
			if (await TableExistsAsync(ct))
			{
				await ExecuteNonQueryAsync($"DELETE FROM {_quotedTargetTableName}", ct);
				if (_logger.IsEnabled(LogLevel.Information)) _logger.LogInformation("Deleted rows from {Table}", _quotedTargetTableName);
			}
			else
			{
				var createTableSql = GetCreateTableSql(_quotedTargetTableName, _columns!);
				await ExecuteNonQueryAsync(createTableSql, ct);
			}
		}
		else if (_options.Strategy == SqliteWriteStrategy.Truncate)
		{
			if (await TableExistsAsync(ct))
			{
				await ExecuteNonQueryAsync(GetTruncateTableSql(_quotedTargetTableName), ct);
				if (_logger.IsEnabled(LogLevel.Information)) _logger.LogInformation("Truncated table {Table}", _quotedTargetTableName);
			}
			else
			{
				var createTableSql = GetCreateTableSql(_quotedTargetTableName, _columns!);
				await ExecuteNonQueryAsync(createTableSql, ct);
			}
		}
		else // Append
		{
			if (!await TableExistsAsync(ct))
			{
				var createTableSql = GetCreateTableSql(_quotedTargetTableName, _columns!);
				await ExecuteNonQueryAsync(createTableSql, ct);
				if (_logger.IsEnabled(LogLevel.Information)) _logger.LogInformation("Created table {Table} (Append strategy)", _quotedTargetTableName);
			}
		}

		// Initialize Keys for Upsert/Ignore
		if (_options.Strategy == SqliteWriteStrategy.Upsert || _options.Strategy == SqliteWriteStrategy.Ignore)
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
	}

	private async Task<bool> TableExistsAsync(CancellationToken ct)
	{
		using var existsCmd = (DbCommand)_connection!.CreateCommand();
		existsCmd.CommandText = $"SELECT name FROM sqlite_master WHERE type='table' AND name='{_options.Table}'";
		return await existsCmd.ExecuteScalarAsync(ct) != null;
	}

	public override async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
	{
		if (rows.Count == 0) return;
		if (_connection!.State != ConnectionState.Open) await ((DbConnection)_connection).OpenAsync(ct);

		try
		{
			await using var transaction = await ((DbConnection)_connection).BeginTransactionAsync(ct);

			var paramNames = string.Join(", ", Enumerable.Range(0, _columns!.Count).Select(i => $"@p{i}"));
			var columnNames = string.Join(", ", _columns.Select(c => SqlIdentifierHelper.GetSafeIdentifier(_dialect, c)));

			using var cmd = (SqliteCommand)_connection.CreateCommand();
			cmd.Transaction = (SqliteTransaction)transaction;

			var sql = new StringBuilder();
			if (_options.Strategy == SqliteWriteStrategy.Ignore)
			{
				sql.Append($"INSERT OR IGNORE INTO {_quotedTargetTableName} ({columnNames}) VALUES ({paramNames})");
			}
			else if (_options.Strategy == SqliteWriteStrategy.Upsert)
			{
				var conflictTarget = string.Join(", ", _keyColumns.Select(k => SqlIdentifierHelper.GetSafeIdentifier(_dialect, k)));
				var updateSet = string.Join(", ", _columns.Where(c => !_keyColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase))
													  .Select(c =>
													  {
														  var safe = SqlIdentifierHelper.GetSafeIdentifier(_dialect, c);
														  return $"{safe} = excluded.{safe}";
													  }));

				sql.Append($"INSERT INTO {_quotedTargetTableName} ({columnNames}) VALUES ({paramNames}) ");
				sql.Append($"ON CONFLICT ({conflictTarget}) DO UPDATE SET {updateSet}");
			}
			else
			{
				sql.Append($"INSERT INTO {_quotedTargetTableName} ({columnNames}) VALUES ({paramNames})");
			}

			cmd.CommandText = sql.ToString();

			for (int i = 0; i < _columns.Count; i++)
			{
				cmd.Parameters.Add(new SqliteParameter($"@p{i}", null));
			}

			foreach (var row in rows)
			{
				for (int i = 0; i < _columns.Count; i++)
				{
					cmd.Parameters[i].Value = row[i] ?? DBNull.Value;
				}
				await cmd.ExecuteNonQueryAsync(ct);
			}

			await transaction.CommitAsync(ct);
		}
		catch (Exception ex)
		{
			var analysis = await BatchFailureAnalyzer.AnalyzeAsync(this, rows, _columns!, ct);
			if (!string.IsNullOrEmpty(analysis))
			{
				throw new InvalidOperationException($"SQLite Insert Failed with detailed analysis:\n{analysis}", ex);
			}
			throw;
		}
	}

	public override async ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
	{
		await EnsureConnectionOpenAsync(ct);

		using var cmd = (DbCommand)_connection!.CreateCommand();
		cmd.CommandText = command;
		await cmd.ExecuteNonQueryAsync(ct);
	}

	protected override ValueTask DisposeResourcesAsync()
	{
		return ValueTask.CompletedTask;
	}

	protected override async Task<TargetSchemaInfo?> InspectTargetInternalAsync(CancellationToken ct = default)
	{
		await using var connection = new SqliteConnection(_connectionString);
		await connection.OpenAsync(ct);

		var tableName = _options.Table;

		// Check if table exists
		using var existsCmd = connection.CreateCommand();
		existsCmd.CommandText = $"SELECT name FROM sqlite_master WHERE type='table' AND name='{tableName}'";
		var exists = await existsCmd.ExecuteScalarAsync(ct) != null;

		if (!exists) return new TargetSchemaInfo([], false, null, null, null);

		// Get columns using PRAGMA table_info
		using var columnsCmd = connection.CreateCommand();
		columnsCmd.CommandText = $"PRAGMA table_info(\"{tableName}\")";

		var pkColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		var columns = new List<TargetColumnInfo>();

		using var reader = await columnsCmd.ExecuteReaderAsync(ct);
		while (await reader.ReadAsync(ct))
		{
			var colName = reader.GetString(1);
			var dataType = reader.GetString(2);
			var notNull = reader.GetInt32(3) == 1;
			var isPk = reader.GetInt32(5) > 0;

			if (isPk) pkColumns.Add(colName);

			columns.Add(new TargetColumnInfo(
				colName,
				dataType.ToUpperInvariant(),
				_typeMapper.MapFromProviderType(dataType),
				!notNull && !isPk,
				isPk,
				false,
				ExtractMaxLength(dataType)
			));
		}

		long? rowCount = null;
		try
		{
			using var countCmd = connection.CreateCommand();
			countCmd.CommandText = $"SELECT COUNT(*) FROM \"{tableName}\"";
			var countResult = await countCmd.ExecuteScalarAsync(ct);
			rowCount = countResult == null ? null : Convert.ToInt64(countResult);
		}
		catch { }

		long? sizeBytes = null;
		try
		{
			var builder = new SqliteConnectionStringBuilder(_connectionString);
			if (!string.IsNullOrEmpty(builder.DataSource) && File.Exists(builder.DataSource))
			{
				sizeBytes = new FileInfo(builder.DataSource).Length;
			}
		}
		catch { }

		return new TargetSchemaInfo(columns, true, rowCount, sizeBytes, pkColumns.Count > 0 ? pkColumns.ToList() : null);
	}

	private static int? ExtractMaxLength(string dataType)
	{
		var match = Regex.Match(dataType, @"\((\d+)\)");
		if (match.Success && int.TryParse(match.Groups[1].Value, out var length)) return length;
		return null;
	}

	protected override string GetCreateTableSql(string tableName, IEnumerable<PipeColumnInfo> columns)
	{
		var sb = new StringBuilder();
		sb.Append($"CREATE TABLE {tableName} (");

		var cols = columns.ToList();
		for (int i = 0; i < cols.Count; i++)
		{
			if (i > 0) sb.Append(", ");
			var col = cols[i];
			sb.Append($"{SqlIdentifierHelper.GetSafeIdentifier(_dialect, col)} {_typeMapper.MapToProviderType(col.ClrType)}");
		}

		if (!string.IsNullOrEmpty(_options.Key))
		{
			var resolvedKeys = ColumnHelper.ResolveKeyColumns(_options.Key, cols);
			var keys = resolvedKeys.Select(keyName => SqlIdentifierHelper.GetSafeIdentifier(_dialect, cols.First(c => c.Name == keyName)));
			sb.Append($", PRIMARY KEY ({string.Join(", ", keys)})");
		}
		sb.Append(')');
		return sb.ToString();
	}

	protected override string GetTruncateTableSql(string tableName) => $"DELETE FROM {tableName}"; // VACUUM is handled outside usually

	protected override string GetDropTableSql(string tableName) => $"DROP TABLE {tableName}";

	protected override string GetAddColumnSql(string tableName, PipeColumnInfo column)
	{
		var safeName = SqlIdentifierHelper.GetSafeIdentifier(_dialect, column.Name);
		var type = _typeMapper.MapToProviderType(column.ClrType);
		var nullability = column.IsNullable ? "" : " NOT NULL";
		return $"ALTER TABLE {tableName} ADD COLUMN {safeName} {type}{nullability}";
	}



	// IKeyValidator Override
	public override string? GetWriteStrategy() => _options.Strategy.ToString();
	public override IReadOnlyList<string>? GetRequestedPrimaryKeys()
	{
		if (string.IsNullOrEmpty(_options.Key)) return null;
		return _options.Key.Split(',').Select(k => k.Trim()).Where(k => !string.IsNullOrEmpty(k)).ToList();
	}
	public override bool RequiresPrimaryKey() => _options.Strategy is SqliteWriteStrategy.Upsert or SqliteWriteStrategy.Ignore;
}
