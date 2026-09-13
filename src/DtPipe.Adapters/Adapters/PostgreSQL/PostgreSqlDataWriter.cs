using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Helpers;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DtPipe.Adapters.PostgreSQL;

public sealed partial class PostgreSqlDataWriter : BaseSqlDataWriter, IColumnarDataWriter
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
    // Per-column converters — built once in EnsureMetaDataInitializedAsync (row-mode path)
    private Func<object?, object?>[]? _converters;
    // Per-column typed writers — built once in EnsureMetaDataInitializedAsync (columnar path)
    // Action<array, rowIndex, importer> — sync Npgsql Write<T>(), zero boxing, zero Task allocation
    private Action<IArrowArray, int, NpgsqlBinaryImporter>[]? _arrowColumnWriters;

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

			if (existingSchema != null && existingSchema.Exists)
			{
				ValidateRecreateCompatibility(_columns!, existingSchema);
			}

			return await ApplyRecreateStrategyAsync(ct);
		}
		else if (_options.Strategy == PostgreSqlWriteStrategy.DeleteThenInsert)
		{
			return await ApplyDeleteThenInsertStrategyAsync(ct);
		}
		else if (_options.Strategy == PostgreSqlWriteStrategy.Truncate)
		{
			return await ApplyTruncateStrategyAsync(ct);
		}
		else
		{
			return await ApplyAppendStrategyAsync(ct);
		}
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

		// Build per-column converters once — eliminates per-cell ConvertValue dispatch (row-mode)
		_converters = new Func<object?, object?>[_targetToSourceMap.Length];
		for (int k = 0; k < _targetToSourceMap.Length; k++)
		{
			var sourceClrType = _columns![_targetToSourceMap[k]].ClrType;
			_converters[k] = CompositeCellJson.Wrap(ColumnConverterFactory.Build(sourceClrType, _targetTypes[k]));
		}

		// Build per-column Arrow writers — sync Npgsql Write<T>(), zero boxing (columnar path)
		_arrowColumnWriters = new Action<IArrowArray, int, NpgsqlBinaryImporter>[_targetToSourceMap.Length];
		for (int k = 0; k < _targetToSourceMap.Length; k++)
		{
			_arrowColumnWriters[k] = BuildNpgsqlColumnWriter(_columnTypes[k]);
		}

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
		await base.ResolveKeysAsync(_keyColumns, ct);
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

	/// <summary>
	/// Columnar write path: receives an Arrow RecordBatch and feeds it directly into
	/// NpgsqlBinaryImporter without materializing an intermediate object?[] row buffer.
	/// Activated automatically by PipelineExecutor when the upstream source is columnar
	/// (Parquet, PostgreSQL, DuckDB SQL output).
	/// </summary>
	public async ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
	{
		if (batch.Length == 0) return;

		await EnsureConnectionOpenAsync(ct);
		await EnsureMetaDataInitializedAsync(ct);

		using (batch)
		{
			// COPY BINARY has no composite form a text column can accept, so a struct or a list
			// column becomes JSON before the column writers see it. Same reference back when the
			// batch holds none, which is the ordinary case.
			using var payload = CompositeCellJson.RenderComposites(batch);

			if (_options.Strategy is PostgreSqlWriteStrategy.Upsert or PostgreSqlWriteStrategy.Ignore)
			{
				await WriteRecordBatchViaStagingAsync(payload.Batch, ct);
			}
			else
			{
				await WriteRecordBatchDirectAsync(payload.Batch, ct);
			}
		}
	}



	/// <summary>
	/// Checks that every source column has a CLR type compatible with the existing target column.
	/// Throws <see cref="InvalidOperationException"/> with a descriptive message on the first
	/// mismatch found, preventing a cryptic <see cref="InvalidCastException"/> later in the
	/// Arrow write path.
	/// </summary>
	private static void ValidateRecreateCompatibility(
		IReadOnlyList<PipeColumnInfo> sourceColumns,
		TargetSchemaInfo existingTarget)
	{
		foreach (var sourceCol in sourceColumns)
		{
			var targetCol = existingTarget.Columns
				.FirstOrDefault(c => c.Name.Equals(sourceCol.Name, StringComparison.OrdinalIgnoreCase));
			if (targetCol?.InferredClrType == null) continue;

			var srcBase = Nullable.GetUnderlyingType(sourceCol.ClrType) ?? sourceCol.ClrType;
			var tgtBase = Nullable.GetUnderlyingType(targetCol.InferredClrType) ?? targetCol.InferredClrType;

			if (srcBase != tgtBase)
			{
				throw new InvalidOperationException(
					$"Recreate strategy: column '{sourceCol.Name}' has incompatible types — " +
					$"source is '{srcBase.Name}' but existing table column is '{tgtBase.Name}' ({targetCol.NativeType}). " +
					$"Drop the table manually before rerunning, or add --pre-exec \"DROP TABLE IF EXISTS <table> CASCADE\".");
			}
		}
	}



	public override async ValueTask CompleteAsync(CancellationToken ct = default)
	{
		await ValueTask.CompletedTask;
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

	protected override string? GetRequestedKeySpec() => _options.Key;

	protected override string? GetRequestedTableSpec() => _options.Table;

	public override bool RequiresPrimaryKey() => _options.Strategy is PostgreSqlWriteStrategy.Upsert or PostgreSqlWriteStrategy.Ignore;
}
