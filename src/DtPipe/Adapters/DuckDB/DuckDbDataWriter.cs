using System.Data;
using System.Text;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Helpers;
using DtPipe.Core.Models;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.DuckDB;

public sealed class DuckDbDataWriter : BaseSqlDataWriter
{
	private readonly DuckDbWriterOptions _options;
	private readonly ILogger<DuckDbDataWriter> _logger;
	private readonly ITypeMapper _typeMapper;
	private string? _stagingTable; // Table to load data into before merging
	private string? _unquotedSchema;
	private string? _unquotedTable;
	private List<string> _keyColumns = new();
	private IDisposable? _appender;
	private int[]? _columnMapping; // Maps: TargetIndex -> SourceIndex (or -1 if missing)
	private Type[]? _targetTypes;  // Types of the target table columns

	private readonly ISqlDialect _dialect = new DtPipe.Core.Dialects.DuckDbDialect();
	public override ISqlDialect Dialect => _dialect;

	public DuckDbDataWriter(string connectionString, DuckDbWriterOptions options, ILogger<DuckDbDataWriter> logger, ITypeMapper typeMapper) : base(connectionString)
	{
		_options = options;
		_logger = logger;
		_typeMapper = typeMapper;
	}

	protected override IDbConnection CreateConnection(string connectionString)
	{
		return new DuckDBConnection(connectionString);
	}

	protected override Task<(string Schema, string Table)> ResolveTargetTableAsync(CancellationToken ct)
	{
		// DuckDB usually uses "schema.table" or just "table" (default schema is main)
		// We do simple parsing if '.' exists, otherwise schema is empty.
		var parts = _options.Table.Split('.');
		if (parts.Length == 2)
		{
			return Task.FromResult((parts[0], parts[1]));
		}
		return Task.FromResult((string.Empty, _options.Table));
	}

	protected override async Task ApplyWriteStrategyAsync(string resolvedSchema, string resolvedTable, CancellationToken ct)
	{
		_unquotedSchema = resolvedSchema;
		_unquotedTable = resolvedTable;

		// _quotedTargetTableName is already set by BaseSqlDataWriter using Dialect.Quote if needed.

		if (_options.Strategy == DuckDbWriteStrategy.Recreate)
		{
			// 0. Introspect BEFORE Drop to preserve native types (Introspect-Before-Drop)
			TargetSchemaInfo? existingSchema = null;
			try
			{
				existingSchema = await InspectTargetAsync(ct);
			}
			catch
			{
				// Ignore introspection failures
			}

			// Drop
			await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_quotedTargetTableName}", ct);
			if (_logger.IsEnabled(LogLevel.Information)) _logger.LogInformation("Dropped table {Table}", _quotedTargetTableName);

			// 1. Recreate
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
		else if (_options.Strategy == DuckDbWriteStrategy.DeleteThenInsert)
		{
			// Check if table exists before deleting
			if (await TableExistsAsync(ct))
			{
				await ExecuteNonQueryAsync($"DELETE FROM {_quotedTargetTableName}", ct);
				if (_logger.IsEnabled(LogLevel.Information)) _logger.LogInformation("Deleted rows from {Table}", _quotedTargetTableName);
			}
			else
			{
				// Create if not exists
				var createTableSql = GetCreateTableSql(_quotedTargetTableName, _columns!);
				await ExecuteNonQueryAsync(createTableSql, ct);
			}
		}
		else if (_options.Strategy == DuckDbWriteStrategy.Truncate)
		{
			if (await TableExistsAsync(ct))
			{
				await ExecuteNonQueryAsync(GetTruncateTableSql(_quotedTargetTableName), ct);
				if (_logger.IsEnabled(LogLevel.Information)) _logger.LogInformation("Truncated table {Table}", _quotedTargetTableName);
			}
			else
			{
				// Create if not exists
				var createTableSql = GetCreateTableSql(_quotedTargetTableName, _columns!);
				await ExecuteNonQueryAsync(createTableSql, ct);
			}
		}
		else
		{
			// Append / Upsert / Ignore: Ensure table exists
			if (!await TableExistsAsync(ct))
			{
				var createTableSql = GetCreateTableSql(_quotedTargetTableName, _columns!);
				await ExecuteNonQueryAsync(createTableSql, ct);
			}
		}

		// Incremental Loading Setup (Upsert/Ignore)
		if (_options.Strategy == DuckDbWriteStrategy.Upsert || _options.Strategy == DuckDbWriteStrategy.Ignore)
		{
			await SetupUpsertStagingAsync(ct);
		}
	}

	private async Task<bool> TableExistsAsync(CancellationToken ct)
	{
		// Simple check
		var checkCmd = ((DuckDBConnection)_connection!).CreateCommand();
		// Quote handling for string literal in SQL is annoying, but DuckDB is somewhat flexible.
		// Better to use parameter? But system tables?
		// information_schema.tables works standardly.

		// We can reuse InspectTargetAsync but it might be heavy.
		// Let's use a quick COUNT check.
		// Note: _options.Table might be "schema.table".
		// information_schema.tables wants table_name and table_schema.

		// Parsing again:
		var parts = _options.Table.Split('.');
		string schemaVal = parts.Length > 1 ? parts[0] : "main";
		string tableVal = parts.Length > 1 ? parts[1] : _options.Table;

		checkCmd.CommandText = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{schemaVal}' AND table_name = '{tableVal}'";
		var res = await checkCmd.ExecuteScalarAsync(ct);
		return Convert.ToInt32(res) > 0;
	}

	private async Task SetupUpsertStagingAsync(CancellationToken ct)
	{
		// 1. Keys
		TargetSchemaInfo? targetInfoKeys = await InspectTargetAsync(ct);

		if (targetInfoKeys?.PrimaryKeyColumns != null)
		{
			_keyColumns.AddRange(targetInfoKeys.PrimaryKeyColumns);
		}

		if (_keyColumns.Count == 0 && string.IsNullOrEmpty(_options.Key))
		{
			throw new InvalidOperationException($"Strategy {_options.Strategy} requires a Primary Key. None detected and none specified.");
		}

		if (!string.IsNullOrEmpty(_options.Key))
		{
			_keyColumns.Clear();
			_keyColumns.AddRange(ColumnHelper.ResolveKeyColumns(_options.Key, _columns!));
		}

		// 2. Create Staging Table
		_stagingTable = $"{_options.Table}_stage_{Guid.NewGuid():N}";
		// Create stage with same schema as target
		await ExecuteNonQueryAsync($"CREATE TABLE {_stagingTable} AS SELECT * FROM {_quotedTargetTableName} WHERE 1=0", ct);
	}

	protected override async ValueTask OnInitializedAsync(CancellationToken ct)
	{
		// Compute mapping: The Appender expects values in the PHYSICAL order of the table.
		var targetInfo = await InspectTargetAsync(ct);
		if (targetInfo != null && targetInfo.Columns.Count > 0)
		{
			_columnMapping = new int[targetInfo.Columns.Count];
			_targetTypes = new Type[targetInfo.Columns.Count];
			for (int i = 0; i < targetInfo.Columns.Count; i++)
			{
				var targetCol = targetInfo.Columns[i];
				var targetName = targetCol.Name;
				_targetTypes[i] = targetCol.InferredClrType ?? typeof(string);

				var sourceIndex = -1;
				// Find corresponding source column
				for (int s = 0; s < _columns!.Count; s++)
				{
					if (string.Equals(_columns[s].Name, targetName, StringComparison.OrdinalIgnoreCase))
					{
						sourceIndex = s;
						break;
					}
				}
				_columnMapping[i] = sourceIndex;
			}
		}
		else
		{
			// Fallback to 1:1
			_columnMapping = Enumerable.Range(0, _columns!.Count).ToArray();
			_targetTypes = _columns.Select(c => c.ClrType).ToArray();
		}

		// Initialize Appender
		// If Staging, append to stage. Else append to target.
		// Stage table is internal so usually just name, but safe to quote if needed.
		// DuckDB .NET client CreateAppender expects qualified table name but usually without quotes
		// unless you want a literal quote in the name.

		var targetForAppender = _stagingTable ?? (string.IsNullOrEmpty(_unquotedSchema) ? _unquotedTable! : $"{_unquotedSchema}.{_unquotedTable}");

		_appender = ((DuckDBConnection)_connection!).CreateAppender(targetForAppender);
	}

	public override async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
	{
		if (_columns is null || _appender is null || _columnMapping is null) throw new InvalidOperationException("Not initialized");

		try
		{
			await Task.Run(() =>
			{
				var appender = (DuckDBAppender)_appender;
				foreach (var rowData in rows)
				{
					var row = appender.CreateRow();

					for (int i = 0; i < _columnMapping.Length; i++)
					{
						var sourceIndex = _columnMapping[i];

						if (sourceIndex == -1)
						{
							row.AppendNullValue();
						}
						else
						{
							var val = rowData[sourceIndex];
							var targetType = _targetTypes![i];

							if (val is null || val == DBNull.Value)
							{
								row.AppendNullValue();
							}
							else
							{
								AppendValue(row, val, targetType);
							}
						}
					}
					row.EndRow();
				}
			}, ct);
		}
		catch (Exception ex)
		{
			var analysis = await BatchFailureAnalyzer.AnalyzeAsync(this, rows, _columns, ct);
			if (!string.IsNullOrEmpty(analysis))
			{
				throw new InvalidOperationException($"DuckDB Appender Failed with detailed analysis:\n{analysis}", ex);
			}
			throw new InvalidOperationException($"DuckDB Appender Failed: {ex.Message}", ex);
		}
	}

	private void AppendValue(IDuckDBAppenderRow row, object val, Type type)
	{
		var underlying = Nullable.GetUnderlyingType(type) ?? type;

		object convertedVal;
		try
		{
			convertedVal = ValueConverter.ConvertValue(val, underlying);
		}
		catch
		{
			// Fallback to original value if conversion fails, let DuckDB throw if it still dislikes it
			convertedVal = val;
		}

		if (underlying == typeof(int)) row.AppendValue((int)convertedVal);
		else if (underlying == typeof(long)) row.AppendValue((long)convertedVal);
		else if (underlying == typeof(short)) row.AppendValue((short)convertedVal);
		else if (underlying == typeof(byte)) row.AppendValue((byte)convertedVal);
		else if (underlying == typeof(bool)) row.AppendValue((bool)convertedVal);
		else if (underlying == typeof(float)) row.AppendValue((float)convertedVal);
		else if (underlying == typeof(double)) row.AppendValue((double)convertedVal);
		else if (underlying == typeof(decimal)) row.AppendValue((decimal)convertedVal);
		else if (underlying == typeof(DateTime)) row.AppendValue((DateTime)convertedVal);
		else if (underlying == typeof(DateTimeOffset)) row.AppendValue((DateTimeOffset)convertedVal);
		else if (underlying == typeof(Guid)) row.AppendValue((Guid)convertedVal);
		else if (underlying == typeof(byte[])) row.AppendValue((byte[])convertedVal);
		else row.AppendValue(convertedVal.ToString() ?? "");
	}


	public override async ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
	{
		await EnsureConnectionOpenAsync(ct);

		using var cmd = (DuckDBCommand)_connection!.CreateCommand();
		cmd.CommandText = command;
		await cmd.ExecuteNonQueryAsync(ct);
	}

	public override async ValueTask CompleteAsync(CancellationToken ct = default)
	{
		_appender?.Dispose();
		_appender = null;

		if (_stagingTable != null)
		{
			try
			{
				// Perform Merge
				// Construct ON CONFLICT clause
				var conflictTargetParts = new List<string>();
				foreach (var k in _keyColumns)
				{
					var col = _columns!.FirstOrDefault(c => c.Name.Equals(k, StringComparison.OrdinalIgnoreCase));
					if (col != null)
					{
						conflictTargetParts.Add(SqlIdentifierHelper.GetSafeIdentifier(_dialect, col));
					}
					else
					{
						conflictTargetParts.Add(SqlIdentifierHelper.GetSafeIdentifier(_dialect, k));
					}
				}
				var conflictTarget = string.Join(", ", conflictTargetParts);

				var updateSet = string.Join(", ", _columns!.Where(c => !_keyColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase))
													  .Select(c =>
													  {
														  var safe = SqlIdentifierHelper.GetSafeIdentifier(_dialect, c);
														  return $"{safe} = EXCLUDED.{safe}";
													  }));

				var sql = new StringBuilder();
				sql.Append($"INSERT INTO {_quotedTargetTableName} SELECT * FROM {_stagingTable} ");

				if (_options.Strategy == DuckDbWriteStrategy.Ignore)
				{
					sql.Append($"ON CONFLICT ({conflictTarget}) DO NOTHING");
				}
				else if (_options.Strategy == DuckDbWriteStrategy.Upsert)
				{
					sql.Append($"ON CONFLICT ({conflictTarget}) DO UPDATE SET {updateSet}");
				}

				await ExecuteNonQueryAsync(sql.ToString(), ct);
			}
			finally
			{
				await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_stagingTable}", ct);
			}
		}
	}

	protected override ValueTask DisposeResourcesAsync()
	{
		_appender?.Dispose();
		_appender = null;
		return ValueTask.CompletedTask;
	}

	// Abstract implementations

	public override async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
	{
		await using var connection = new DuckDBConnection(_connectionString);
		await connection.OpenAsync(ct);

		// Resolve table/schema if not done yet
		var schemaName = _unquotedSchema;
		var tableName = _unquotedTable;
		if (tableName == null)
		{
			var parts = _options.Table.Split('.');
			if (parts.Length == 2) { schemaName = parts[0]; tableName = parts[1]; }
			else { schemaName = "main"; tableName = _options.Table; }
		}
		if (string.IsNullOrEmpty(schemaName)) schemaName = "main";

		var quotedName = BuildQuotedTableName(schemaName, tableName);

		// Check if table exists
		using var existsCmd = connection.CreateCommand();
		existsCmd.CommandText = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{schemaName}' AND table_name = '{tableName}'";

		var exists = Convert.ToInt32(await existsCmd.ExecuteScalarAsync(ct)) > 0;

		if (!exists) return new TargetSchemaInfo([], false, null, null, null);

		using var columnsCmd = connection.CreateCommand();
		// PRAGMA table_info expects the table name, optionally qualified.
		// If it's in a different schema, we might need 'schema.table'.
		var pragmaTarget = schemaName == "main" ? tableName : $"{schemaName}.{tableName}";
		columnsCmd.CommandText = $"PRAGMA table_info('{pragmaTarget}')";

		long? rowCount = null;
		try
		{
			using var countCmd = connection.CreateCommand();
			countCmd.CommandText = $"SELECT COUNT(*) FROM {quotedName}";
			var countResult = await countCmd.ExecuteScalarAsync(ct);
			rowCount = countResult == null ? null : Convert.ToInt64(countResult);
		}
		catch { }

		var pkColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		var columns = new List<TargetColumnInfo>();

		using var reader = await columnsCmd.ExecuteReaderAsync(ct);
		while (await reader.ReadAsync(ct))
		{
			var colName = reader.GetString(1);
			var dataType = reader.GetString(2);
			var notNull = reader.GetBoolean(3);
			var isPk = reader.GetBoolean(5);

			if (isPk) pkColumns.Add(colName);

			columns.Add(new TargetColumnInfo(
				colName,
				dataType.ToUpperInvariant(),
				_typeMapper.MapFromProviderType(dataType),
				!notNull,
				isPk,
				false,
				null
			));
		}

		return new TargetSchemaInfo(
			columns,
			true,
			rowCount,
			null,
			pkColumns.Count > 0 ? pkColumns.ToList() : null
		);
	}

	protected override string GetCreateTableSql(string tableName, IEnumerable<PipeColumnInfo> columns)
	{
		var sb = new StringBuilder();
		// We don't use IF NOT EXISTS here because Base logic might control it,
		// BUT for standard Create strategy it relies on this returning the CREATE statement.
		sb.Append($"CREATE TABLE {tableName} (");

		var colsList = columns.ToList();
		for (int i = 0; i < colsList.Count; i++)
		{
			if (i > 0) sb.Append(", ");
			sb.Append($"{SqlIdentifierHelper.GetSafeIdentifier(_dialect, colsList[i])} {_typeMapper.MapToProviderType(colsList[i].ClrType)}");
		}

		if (!string.IsNullOrEmpty(_options.Key))
		{
			var keys = _options.Key.Split(',').Select(k => SqlIdentifierHelper.GetSafeIdentifier(_dialect, k.Trim()));
			sb.Append($", PRIMARY KEY ({string.Join(", ", keys)})");
		}

		sb.Append(")");
		return sb.ToString();
	}

	protected override string GetTruncateTableSql(string tableName) => $"TRUNCATE TABLE {tableName}";
	protected override string GetDropTableSql(string tableName) => $"DROP TABLE {tableName}";

	private string BuildCreateTableFromIntrospection(string tableName, TargetSchemaInfo schema)
	{
		var sb = new StringBuilder();
		sb.Append($"CREATE TABLE {tableName} (");

		for (int i = 0; i < schema.Columns.Count; i++)
		{
			var col = schema.Columns[i];
			if (i > 0) sb.Append(", ");
			sb.Append($"{SqlIdentifierHelper.GetSafeIdentifier(_dialect, col.Name)} {col.NativeType}");

			if (!col.IsNullable)
			{
				sb.Append(" NOT NULL");
			}
		}

		if (schema.PrimaryKeyColumns != null && schema.PrimaryKeyColumns.Count > 0)
		{
			var keys = schema.PrimaryKeyColumns.Select(k => SqlIdentifierHelper.GetSafeIdentifier(_dialect, k));
			sb.Append($", PRIMARY KEY ({string.Join(", ", keys)})");
		}

		sb.Append(")");
		return sb.ToString();
	}

	// IKeyValidator Override
	public override string? GetWriteStrategy() => _options.Strategy.ToString();
	public override IReadOnlyList<string>? GetRequestedPrimaryKeys()
	{
		if (string.IsNullOrEmpty(_options.Key)) return null;
		return _options.Key.Split(',').Select(k => k.Trim()).Where(k => !string.IsNullOrEmpty(k)).ToList();
	}
	public override bool RequiresPrimaryKey() => _options.Strategy is DuckDbWriteStrategy.Upsert or DuckDbWriteStrategy.Ignore;
}
