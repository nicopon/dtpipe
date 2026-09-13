using System.Data;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ado;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Helpers;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using MySqlConnector;

namespace DtPipe.Adapters.MySql;

public sealed partial class MySqlDataWriter : BaseSqlDataWriter, IColumnarDataWriter
{
	private readonly MySqlWriterOptions _options;
	private readonly ILogger<MySqlDataWriter> _logger;
	private readonly ITypeMapper _typeMapper;

	private readonly ISqlDialect _dialect = new DtPipe.Core.Dialects.MySqlDialect();
	public override ISqlDialect Dialect => _dialect;

	private readonly List<string> _keyColumns = new();

	/// <summary>Target column names actually written, in buffer order.</summary>
	private string[]? _targetNames;
	private Type[]? _targetTypes;
	private int[]? _sourceIndices;
	private Func<object?, object?>[]? _converters;
	private DataTable? _bufferTable;

	/// <summary>Null until probed; see <see cref="ResolveBulkPathAsync"/>.</summary>
	private bool? _bulkAvailable;

	/// <summary>
	/// True when a PRIMARY KEY or UNIQUE index covers exactly the resolved key columns. MySQL's
	/// ON DUPLICATE KEY UPDATE keys off the table's indexes, never off a caller-supplied conflict
	/// target, so without this the clause is a silent no-op that inserts duplicates.
	/// </summary>
	private bool _constraintVerified;

	public override bool RequiresTargetInspection => _options.Strategy != MySqlWriteStrategy.Recreate;

	protected override ITypeMapper GetTypeMapper() => _typeMapper;

	public MySqlDataWriter(string connectionString, MySqlWriterOptions options, ILogger<MySqlDataWriter> logger, ITypeMapper typeMapper)
		: base(MySqlConnectionHelper.EnableLocalInfile(connectionString))
	{
		_options = options;
		_logger = logger;
		_typeMapper = typeMapper;
	}

	protected override IDbConnection CreateConnection(string connectionString) => new MySqlConnection(connectionString);

	/// <summary>
	/// MySQL has no schema layer below the database, so "schema" here means the database name.
	/// An unqualified table resolves against the connection's default database, which is what
	/// leaving the schema empty achieves — <see cref="BaseSqlDataWriter.BuildQuotedTableName"/>
	/// emits the bare table name in that case.
	/// </summary>
	protected override Task<(string Schema, string Table)> ResolveTargetTableAsync(CancellationToken ct)
	{
		var raw = (_options.Table ?? string.Empty).Trim();
		var parts = raw.Split('.', 2);
		return Task.FromResult(parts.Length == 2
			? (parts[0].Trim('`'), parts[1].Trim('`'))
			: (string.Empty, raw.Trim('`')));
	}

	protected override async Task<TargetSchemaInfo?> ApplyWriteStrategyAsync(string resolvedSchema, string resolvedTable, CancellationToken ct)
	{
		var result = _options.Strategy switch
		{
			MySqlWriteStrategy.Recreate => await ApplyRecreateStrategyAsync(ct),
			MySqlWriteStrategy.Truncate => await ApplyTruncateStrategyAsync(ct),
			MySqlWriteStrategy.DeleteThenInsert => await ApplyDeleteThenInsertStrategyAsync(ct),
			_ => await ApplyAppendStrategyAsync(ct),
		};

		if (_options.Strategy is MySqlWriteStrategy.Upsert or MySqlWriteStrategy.Ignore)
		{
			await ResolveKeysAsync(_keyColumns, ct);
			await VerifyKeyConstraintAsync(ct);
		}

		return result;
	}

	/// <summary>
	/// Confirms that some unique index matches the resolved keys exactly. A strict subset would
	/// make the upsert collide on fewer columns than asked; a strict superset would never collide
	/// at all. Only an exact match makes ON DUPLICATE KEY UPDATE mean what --key said.
	/// </summary>
	private async Task VerifyKeyConstraintAsync(CancellationToken ct)
	{
		_constraintVerified = false;
		if (_keyColumns.Count == 0) return;

		var wanted = new HashSet<string>(_keyColumns, StringComparer.OrdinalIgnoreCase);
		foreach (var index in await LoadUniqueIndexesAsync(ct))
		{
			if (index.Value.Count == wanted.Count && wanted.SetEquals(index.Value))
			{
				_constraintVerified = true;
				return;
			}
		}

		_logger.LogWarning(
			"MySQL: target table {Table} has no PRIMARY KEY or UNIQUE index matching the key columns ({Keys}). " +
			"INSERT ... ON DUPLICATE KEY UPDATE cannot detect the conflict, so {Strategy} falls back to an " +
			"explicit DELETE+INSERT. Add a unique index on those columns to use the fast path.",
			_quotedTargetTableName, string.Join(", ", _keyColumns), _options.Strategy);
	}

	// ── Write paths ──────────────────────────────────────────────────────────

	public override async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
	{
		if (rows.Count == 0) return;

		await EnsureConnectionOpenAsync(ct);
		await EnsureBufferInitializedAsync(ct);

		if (_options.Strategy is MySqlWriteStrategy.Upsert or MySqlWriteStrategy.Ignore)
		{
			await WriteViaStagingAsync(async (destination, token) =>
			{
				FillBufferRows(rows);
				await LoadBufferAsync(destination, token);
			}, ct);
			return;
		}

		FillBufferRows(rows);
		await LoadBufferAsync(_quotedTargetTableName, ct);
	}

	public async ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
	{
		using (batch)
		{
			if (batch.Length == 0) return;

			await EnsureConnectionOpenAsync(ct);
			await EnsureBufferInitializedAsync(ct);

			// The Arrow path can only skip the DataTable when bulk copy is available: the
			// parameterized fallback needs random access to build its VALUES tuples, which a
			// forward-only RecordBatchDataReader does not offer. Materializing then costs one
			// buffer pass — the price of not having LOAD DATA at all.
			// LOAD DATA serialises each cell through RecordBatchDataReader, which hands a struct
			// column over as a Dictionary MySqlConnector refuses. Render those to JSON first.
			using var payload = CompositeCellJson.RenderComposites(batch);

			if (await ResolveBulkPathAsync(ct))
			{
				if (_options.Strategy is MySqlWriteStrategy.Upsert or MySqlWriteStrategy.Ignore)
				{
					await WriteViaStagingAsync(async (destination, token) =>
					{
						using var reader = new RecordBatchDataReader(payload.Batch);
						await BulkCopyAsync(destination, reader, token);
					}, ct);
				}
				else
				{
					using var reader = new RecordBatchDataReader(payload.Batch);
					await BulkCopyAsync(_quotedTargetTableName, reader, ct);
				}
				return;
			}

			await WriteBatchAsync(MaterializeRows(payload.Batch), ct);
		}
	}

	/// <summary>
	/// Runs <paramref name="load"/> against a temporary staging table shaped like the target, then
	/// applies the dialect's merge script. Bulk copy has no upsert mode of its own — MySqlBulkCopy's
	/// Replace conflict option overwrites unlisted columns with defaults, which is not an upsert —
	/// so the merge has to happen in SQL, on a table the load can target freely.
	/// </summary>
	private async Task WriteViaStagingAsync(Func<string, CancellationToken, Task> load, CancellationToken ct)
	{
		var stagingTable = _dialect.Quote($"dtp_stage_{Guid.NewGuid():N}");

		// TEMPORARY keeps the table scoped to this connection, so concurrent branches writing the
		// same target never collide, and a crashed process leaves nothing behind to clean up.
		await ExecuteNonQueryAsync($"CREATE TEMPORARY TABLE {stagingTable} LIKE {_quotedTargetTableName}", ct);

		try
		{
			await load(stagingTable, ct);

			var spec = new MergeSpec(
				QuotedTargetTable: _quotedTargetTableName,
				SourceTable: stagingTable,
				KeyColumns: _keyColumns,
				Columns: _targetNames!.Select(n => new PipeColumnInfo(n, typeof(object), false)).ToList(),
				Mode: _options.Strategy == MySqlWriteStrategy.Ignore ? MergeMode.Ignore : MergeMode.Upsert,
				ConstraintVerified: _constraintVerified);

			foreach (var statement in _dialect.BuildStagingMerge(spec)
						 .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
			{
				await ExecuteNonQueryAsync(statement, ct);
			}
		}
		finally
		{
			try { await ExecuteNonQueryAsync($"DROP TEMPORARY TABLE IF EXISTS {stagingTable}", ct); }
			catch (Exception ex) { _logger.LogWarning(ex, "Failed to drop staging table {TableName}", stagingTable); }
		}
	}

	private async Task LoadBufferAsync(string destination, CancellationToken ct)
	{
		if (await ResolveBulkPathAsync(ct))
		{
			using var reader = _bufferTable!.CreateDataReader();
			await BulkCopyAsync(destination, reader, ct);
			return;
		}

		await InsertBufferAsync(destination, ct);
	}

	private async Task BulkCopyAsync(string destination, IDataReader reader, CancellationToken ct)
	{
		var bulk = new MySqlBulkCopy((MySqlConnection)_connection!)
		{
			DestinationTableName = destination,
			BulkCopyTimeout = 0,
		};

		for (var i = 0; i < _targetNames!.Length; i++)
			bulk.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(i, _targetNames[i]));

		var result = await bulk.WriteToServerAsync(reader, ct);

		// MySqlBulkCopy reports per-row problems as warnings instead of throwing, because
		// LOAD DATA itself does. Surfacing them keeps a partially-rejected load from being
		// reported as a clean success.
		if (result.Warnings.Count > 0)
		{
			throw new InvalidOperationException(
				$"MySQL bulk load into {destination} reported {result.Warnings.Count} warning(s); " +
				$"first: {result.Warnings[0].Message}");
		}
	}

	/// <summary>
	/// Batched multi-row INSERT, used when LOAD DATA LOCAL INFILE is unavailable. Statements are
	/// capped by placeholder count rather than row count: MySQL's protocol limit is on the number
	/// of parameters, so the safe row count depends on how wide the table is.
	/// </summary>
	private async Task InsertBufferAsync(string destination, CancellationToken ct)
	{
		const int MaxPlaceholdersPerStatement = 2000;

		var colCount = _targetNames!.Length;
		var rowsPerStatement = Math.Max(1, MaxPlaceholdersPerStatement / Math.Max(1, colCount));
		var columnList = string.Join(", ", _targetNames.Select(_dialect.Quote));

		await using var transaction = await ((MySqlConnection)_connection!).BeginTransactionAsync(ct);
		try
		{
			for (var offset = 0; offset < _bufferTable!.Rows.Count; offset += rowsPerStatement)
			{
				var count = Math.Min(rowsPerStatement, _bufferTable.Rows.Count - offset);

				var sql = new StringBuilder($"INSERT INTO {destination} ({columnList}) VALUES ");
				using var cmd = (MySqlCommand)_connection.CreateCommand();
				cmd.Transaction = (MySqlTransaction)transaction;

				for (var r = 0; r < count; r++)
				{
					if (r > 0) sql.Append(", ");
					sql.Append('(');
					for (var c = 0; c < colCount; c++)
					{
						if (c > 0) sql.Append(", ");
						var name = $"@p{r}_{c}";
						sql.Append(name);
						cmd.Parameters.AddWithValue(name, _bufferTable.Rows[offset + r][c] ?? DBNull.Value);
					}
					sql.Append(')');
				}

				cmd.CommandText = sql.ToString();
				await cmd.ExecuteNonQueryAsync(ct);
			}

			await transaction.CommitAsync(ct);
		}
		catch
		{
			await transaction.RollbackAsync(ct);
			throw;
		}
	}

	/// <summary>
	/// Decides once whether the bulk path is usable. <c>MySqlBulkCopy</c> is LOAD DATA LOCAL
	/// INFILE underneath, which needs the client flag (set in the connection string) *and* the
	/// server's local_infile, OFF by default since MySQL 8 and changeable only with SUPER. Probing
	/// it turns an obscure mid-stream driver error into one decision and one warning.
	/// </summary>
	private async Task<bool> ResolveBulkPathAsync(CancellationToken ct)
	{
		if (_bulkAvailable.HasValue) return _bulkAvailable.Value;

		if (_options.InsertMode == MySqlInsertMode.Standard)
		{
			_bulkAvailable = false;
			return false;
		}

		var enabled = false;
		try
		{
			using var cmd = (MySqlCommand)_connection!.CreateCommand();
			cmd.CommandText = "SELECT @@GLOBAL.local_infile";
			var value = await cmd.ExecuteScalarAsync(ct);
			enabled = value is not null && value != DBNull.Value && Convert.ToInt64(value) == 1;
		}
		catch (MySqlException ex)
		{
			_logger.LogDebug(ex, "MySQL: could not read @@GLOBAL.local_infile; assuming bulk load is unavailable.");
		}

		if (!enabled)
		{
			_logger.LogWarning(
				"MySQL: server has local_infile=OFF, so LOAD DATA LOCAL INFILE (the bulk path) is refused. " +
				"Falling back to batched multi-row INSERT. Set local_infile=ON on the server to enable bulk loading.");
		}

		_bulkAvailable = enabled;
		return enabled;
	}

	// ── Buffer plumbing ──────────────────────────────────────────────────────

	/// <summary>
	/// Builds the write buffer over the columns the source and target actually share.
	/// <para>
	/// Deliberately an intersection, not the full target column list: a target column the source
	/// does not carry is left out of the INSERT so its DEFAULT applies. Padding it with NULL
	/// instead would fail outright on a NOT NULL column that has a perfectly good default.
	/// </para>
	/// </summary>
	private async Task EnsureBufferInitializedAsync(CancellationToken ct)
	{
		if (_bufferTable != null) return;

		var targetInfo = await InspectTargetAsync(ct);
		var targetColumns = targetInfo?.Columns;

		var names = new List<string>();
		var types = new List<Type>();
		var sourceIndices = new List<int>();

		if (targetColumns is { Count: > 0 })
		{
			foreach (var targetCol in targetColumns)
			{
				var srcIdx = IndexOfSourceColumn(targetCol.Name);
				if (srcIdx < 0) continue;
				names.Add(targetCol.Name);
				types.Add(targetCol.InferredClrType ?? _columns![srcIdx].ClrType);
				sourceIndices.Add(srcIdx);
			}
		}

		// No introspection (freshly recreated table, or a target we could not read): the source
		// schema is the only description available, and it is the one the CREATE TABLE used.
		if (names.Count == 0)
		{
			for (var i = 0; i < _columns!.Count; i++)
			{
				names.Add(_columns[i].Name);
				types.Add(_columns[i].ClrType);
				sourceIndices.Add(i);
			}
		}

		_targetNames = names.ToArray();
		_targetTypes = types.ToArray();
		_sourceIndices = sourceIndices.ToArray();

		_bufferTable = new DataTable();
		_converters = new Func<object?, object?>[names.Count];
		for (var i = 0; i < names.Count; i++)
		{
			_bufferTable.Columns.Add(names[i], Nullable.GetUnderlyingType(_targetTypes[i]) ?? _targetTypes[i]);
			_converters[i] = CompositeCellJson.Wrap(ColumnConverterFactory.Build(_columns![_sourceIndices[i]].ClrType, _targetTypes[i]));
		}
	}

	private int IndexOfSourceColumn(string name)
	{
		for (var i = 0; i < _columns!.Count; i++)
		{
			if (string.Equals(_columns[i].Name, name, StringComparison.OrdinalIgnoreCase)) return i;
		}
		return -1;
	}

	private void FillBufferRows(IReadOnlyList<object?[]> rows)
	{
		_bufferTable!.Clear();
		foreach (var row in rows)
		{
			var dataRow = _bufferTable.NewRow();
			for (var i = 0; i < _converters!.Length; i++)
			{
				var srcIdx = _sourceIndices![i];
				dataRow[i] = srcIdx >= 0 && srcIdx < row.Length ? _converters[i](row[srcIdx]) : DBNull.Value;
			}
			_bufferTable.Rows.Add(dataRow);
		}
	}

	private static List<object?[]> MaterializeRows(RecordBatch batch)
	{
		using var reader = new RecordBatchDataReader(batch);
		var rows = new List<object?[]>(batch.Length);
		while (reader.Read())
		{
			var row = new object?[reader.FieldCount];
			for (var i = 0; i < row.Length; i++)
				row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
			rows.Add(row);
		}
		return rows;
	}

	// ── DDL ──────────────────────────────────────────────────────────────────

	/// <summary>
	/// MySQL cannot index a LONGTEXT column without a prefix length, so a string column that takes
	/// part in the PRIMARY KEY is narrowed to VARCHAR. Non-key string columns keep LONGTEXT.
	/// Same shape as the SQL Server writer's NVARCHAR(MAX) → NVARCHAR(450) substitution.
	/// </summary>
	protected override string GetCreateTableSql(string tableName, IEnumerable<PipeColumnInfo> columns)
	{
		var requestedKeys = GetRequestedPrimaryKeys();
		var keySet = requestedKeys != null
			? new HashSet<string>(requestedKeys, StringComparer.OrdinalIgnoreCase)
			: new HashSet<string>(StringComparer.OrdinalIgnoreCase);

		var colList = columns.ToList();
		var sb = new StringBuilder();
		sb.Append($"CREATE TABLE {tableName} (");

		for (var i = 0; i < colList.Count; i++)
		{
			if (i > 0) sb.Append(", ");
			var col = colList[i];
			var safeName = SqlIdentifierHelper.GetSafeIdentifier(Dialect, col);
			var nativeType = GetTypeMapper().MapToProviderType(col.ClrType);
			if (keySet.Contains(col.Name) && nativeType.Equals("LONGTEXT", StringComparison.OrdinalIgnoreCase))
				nativeType = MySqlTypeConverter.KeyStringType;
			// A PRIMARY KEY member cannot be nullable in MySQL; it would be silently coerced to
			// NOT NULL anyway, so stating it keeps the DDL and the resulting schema in agreement.
			var nullability = keySet.Contains(col.Name) ? " NOT NULL" : "";
			sb.Append($"{safeName} {nativeType}{nullability}");
		}

		if (requestedKeys != null && requestedKeys.Count > 0)
		{
			var resolvedKeys = ColumnHelper.ResolveKeyColumns(string.Join(",", requestedKeys), colList);
			var safeKeys = resolvedKeys.Select(keyName =>
			{
				var col = colList.First(c => c.Name == keyName);
				return SqlIdentifierHelper.GetSafeIdentifier(Dialect, col);
			});
			sb.Append($", PRIMARY KEY ({string.Join(", ", safeKeys)})");
		}

		sb.Append(')');
		return sb.ToString();
	}

	protected override string GetTruncateTableSql(string tableName) => $"TRUNCATE TABLE {tableName}";
	protected override string GetDropTableSql(string tableName) => $"DROP TABLE {tableName}";

	protected override ValueTask DisposeResourcesAsync()
	{
		_bufferTable?.Dispose();
		_bufferTable = null;
		return ValueTask.CompletedTask;
	}

	// IKeyValidator
	public override string? GetWriteStrategy() => _options.Strategy.ToString();
	protected override string? GetRequestedKeySpec() => _options.Key;
	public override bool RequiresPrimaryKey() => _options.Strategy is MySqlWriteStrategy.Upsert or MySqlWriteStrategy.Ignore;
}
