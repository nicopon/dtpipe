using System.Data;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ado;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Helpers;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Expressions;
using DtPipe.Core.Security;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.DuckDB;

/// <summary>
/// A natively columnar data writer for DuckDB that focuses on high-performance Arrow ingestion.
/// Does not support IRowDataWriter; row-based sources are automatically bridged to Arrow by the orchestrator.
/// </summary>
public sealed class DuckDbDataWriter : IColumnarDataWriter, ISchemaInspector, IKeyValidator, ISchemaMigrator
{
    private readonly string _connectionString;
    private readonly DuckHubConnectionInfo _hubInfo;
    private readonly DuckDbWriterOptions _options;
    private readonly ILogger<DuckDbDataWriter> _logger;
    private readonly ITypeMapper _typeMapper;
    private readonly ISqlDialect _dialect = new DtPipe.Core.Dialects.DuckDbDialect();

    public bool RequiresTargetInspection => _options.Strategy != DuckDbWriteStrategy.Recreate;

    private IDbConnection? _connection;
    private bool _initSqlApplied;
    private string? _quotedTargetTableName;
    private string? _stagingTable;
    private string? _unquotedSchema;
    private string? _unquotedTable;
    private IReadOnlyList<PipeColumnInfo>? _columns;
    private TargetSchemaInfo? _cachedSchema;
    private List<string> _keyColumns = new();

    public void InvalidateSchemaCache() => _cachedSchema = null;

    private IDisposable? _appender;
    private int[]? _columnMapping;
    private Type[]? _targetTypes;
    private Func<object?, object?>[]? _cellConverters;

    private readonly IStringContentResolver? _resolver;

    public DuckDbDataWriter(string connectionString, DuckDbWriterOptions options, ILogger<DuckDbDataWriter> logger, ITypeMapper typeMapper, IStringContentResolver? resolver = null)
    {
        _hubInfo = DuckHubConnectionParser.Parse(options.Variant, connectionString);
        _connectionString = connectionString;
        _options = options;
        _logger = logger;
        _typeMapper = typeMapper;
        _resolver = resolver;
    }

    public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        _columns = columns;
        await EnsureConnectionOpenAsync(ct);

        // Resolve Table/Schema
        var parts = _options.Table.Split('.');
        if (parts.Length == 2)
        {
            _unquotedSchema = parts[0];
            _unquotedTable = parts[1];
        }
        else
        {
            _unquotedSchema = null;
            _unquotedTable = _options.Table;
        }
        _quotedTargetTableName = BuildQuotedTableName(_unquotedSchema, _unquotedTable);

        // Apply Write Strategy
        await ApplyWriteStrategyAsync(ct);

        // Initialize Internal Mapping / Appender
        await PrepareAppenderAsync(ct);
    }

    public async ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
    {
        if (_appender is null || _columnMapping is null) throw new InvalidOperationException("Not initialized");

        try
        {
            using var reader = new RecordBatchDataReader(batch);
            await Task.Run(() =>
            {
                var appender = (DuckDBAppender)_appender;
                while (reader.Read())
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
                            var val = reader.GetValue(sourceIndex);
                            if (val == null || val == DBNull.Value)
                                row.AppendNullValue();
                            else
                            {
                                AppendValue(row, val, _cellConverters![i]);
                            }
                        }
                    }
                    row.EndRow();
                }
            }, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DuckDB Arrow Appender Failed: {Message}", ex.Message);
            throw;
        }
    }

    private void AppendValue(IDuckDBAppenderRow row, object val, Func<object?, object?> converter)
    {
        // For Arrow path, RecordBatchDataReader already provides typed values,
        // but we still use ValueConverter to ensure perfect alignment with target types
        // (e.g. if Arrow has Int64 but DB has Int32).
        object? valueToAppend = val;
        try
        {
            valueToAppend = converter(val);
        }
        catch { /* Fallback to original if conversion fails */ }

        switch (valueToAppend)
        {
            case bool b: row.AppendValue(b); break;
            case sbyte sb: row.AppendValue(sb); break;
            case short s: row.AppendValue(s); break;
            case int i: row.AppendValue(i); break;
            case long l: row.AppendValue(l); break;
            case byte b: row.AppendValue(b); break;
            case ushort us: row.AppendValue(us); break;
            case uint ui: row.AppendValue(ui); break;
            case ulong ul: row.AppendValue(ul); break;
            case float f: row.AppendValue(f); break;
            case double d: row.AppendValue(d); break;
            case decimal dec: row.AppendValue(dec); break;
            case string str: row.AppendValue(str); break;
            case DateTime dt: row.AppendValue(dt); break;
            case DateTimeOffset dto: row.AppendValue(dto.DateTime); break;
            case Guid g: row.AppendValue(g); break;
            case byte[] bytes: row.AppendValue(bytes); break;
            default:
                row.AppendValue(valueToAppend?.ToString() ?? string.Empty);
                break;
        }
    }

    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        _appender?.Dispose();
        _appender = null;

        if (_stagingTable != null && _connection != null)
        {
            try
            {
                var targetInfo = await InspectTargetAsync(ct);
                var pkCols = targetInfo?.PrimaryKeyColumns ?? new List<string>();
                bool hasRequiredConstraints = _keyColumns.All(k => pkCols.Contains(k, StringComparer.OrdinalIgnoreCase));

                // F9: SQL generation is dialect-owned — including the DELETE+INSERT
                // fallback when the target lacks matching constraints.
                var mode = _options.Strategy switch
                {
                    DuckDbWriteStrategy.Ignore => MergeMode.Ignore,
                    DuckDbWriteStrategy.Upsert => MergeMode.Upsert,
                    _ => MergeMode.Insert,
                };
                var spec = new MergeSpec(
                    // Initialized in InitializeAsync (BuildQuotedTableName); CompleteAsync
                    // cannot run before it on this writer's lifecycle.
                    QuotedTargetTable: _quotedTargetTableName!,
                    SourceTable: _stagingTable,
                    KeyColumns: _keyColumns,
                    Columns: _columns!,
                    Mode: mode,
                    ConstraintVerified: hasRequiredConstraints && _keyColumns.Count > 0);

                if (!spec.ConstraintVerified)
                {
                    _logger.LogWarning("DuckDB: Target table {Table} lacks a PRIMARY KEY or UNIQUE constraint matching the specified keys ({Keys}). Falling back to a less-optimized manual DELETE+INSERT strategy for {Strategy}.", _quotedTargetTableName, string.Join(", ", _keyColumns), _options.Strategy);
                }

                var script = _dialect.BuildStagingMerge(spec);
                foreach (var statement in script.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                {
                    await ExecuteNonQueryAsync(statement, ct);
                }
            }
            finally
            {
                await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_stagingTable}", ct);
            }
        }
    }

    public async ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
    {
        await EnsureConnectionOpenAsync(ct);
        await ExecuteNonQueryAsync(command, ct);
    }

    public async ValueTask DisposeAsync()
    {
        _appender?.Dispose();
        _appender = null;

        if (_connection != null)
        {
            if (_connection is IAsyncDisposable ad) await ad.DisposeAsync();
            else _connection.Dispose();
            _connection = null;
        }
    }

    #region Internal Logic (Strategy, Connection, DDL)

    private async Task EnsureConnectionOpenAsync(CancellationToken ct)
    {
        if (_connection == null)
        {
            _connection = new DuckDBConnection(_hubInfo.EffectiveConnectionString);
        }

        if (_connection.State != ConnectionState.Open)
        {
            if (_connection is System.Data.Common.DbConnection dbConn) await dbConn.OpenAsync(ct);
            else _connection.Open();

            if (_hubInfo.IsHub && _hubInfo.InitSqlStatements.Length > 0)
            {
                foreach (var stmt in _hubInfo.InitSqlStatements)
                {
                    await ExecuteNonQueryAsync(stmt, ct);
                }
            }

            if (!_initSqlApplied)
            {
                _initSqlApplied = true;
                await DuckInitSqlRunner.RunAsync((System.Data.Common.DbConnection)_connection, _options.InitSql, _resolver, ct);
            }
        }
    }

    private async Task ExecuteNonQueryAsync(string sql, CancellationToken ct)
    {
        if (_connection == null) throw new InvalidOperationException("Connection not open");
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        if (cmd is System.Data.Common.DbCommand dbCmd) await dbCmd.ExecuteNonQueryAsync(ct);
        else cmd.ExecuteNonQuery();
    }

    private async Task ApplyWriteStrategyAsync(CancellationToken ct)
    {
        InvalidateSchemaCache();
        bool exists = await TableExistsAsync(_unquotedSchema!, _unquotedTable!, ct);

        if (_options.Strategy == DuckDbWriteStrategy.Recreate)
        {
            TargetSchemaInfo? existingSchema = null;
            try { existingSchema = await InspectTargetAsync(ct); } catch { }

            await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_quotedTargetTableName}", ct);

            if (existingSchema?.Exists == true && existingSchema.Columns.Count > 0)
            {
                 // Re-creates the table from existing schema meta
                 var sb = new StringBuilder();
                 sb.Append($"CREATE TABLE {_quotedTargetTableName} (");
                 for (int i = 0; i < existingSchema.Columns.Count; i++)
                 {
                     if (i > 0) sb.Append(", ");
                     var col = existingSchema.Columns[i];
                     var safeName = _dialect.Quote(col.Name);
                     var nativeType = _typeMapper.BuildNativeType(col.NativeType, col.MaxLength, col.Precision, col.Scale, col.MaxLength);
                     sb.Append($"{safeName} {nativeType}{(col.IsNullable ? "" : " NOT NULL")}");
                 }

                 var keyCols = string.IsNullOrEmpty(_options.Key) 
                    ? (existingSchema.PrimaryKeyColumns ?? new List<string>())
                    : ColumnHelper.ResolveKeyColumns(_options.Key, _columns!);

                 if (keyCols.Count > 0)
                 {
                     sb.Append($", PRIMARY KEY ({string.Join(", ", keyCols.Select(pk => _dialect.Quote(pk)))})");
                 }
                 sb.Append(")");
                 await ExecuteNonQueryAsync(sb.ToString(), ct);
            }
            else
            {
                var keyCols = string.IsNullOrEmpty(_options.Key) ? null : ColumnHelper.ResolveKeyColumns(_options.Key, _columns!);
                await ExecuteNonQueryAsync(GenerateCreateTableSql(_quotedTargetTableName!, _columns!, keyCols), ct);
            }
        }
        else if (_options.Strategy == DuckDbWriteStrategy.Truncate && exists)
        {
            await ExecuteNonQueryAsync($"TRUNCATE TABLE {_quotedTargetTableName}", ct);
        }
        else if (_options.Strategy == DuckDbWriteStrategy.DeleteThenInsert && exists)
        {
            await ExecuteNonQueryAsync($"DELETE FROM {_quotedTargetTableName}", ct);
        }
        else if (!exists)
        {
            var keyCols = string.IsNullOrEmpty(_options.Key) ? null : ColumnHelper.ResolveKeyColumns(_options.Key, _columns!);
            await ExecuteNonQueryAsync(GenerateCreateTableSql(_quotedTargetTableName!, _columns!, keyCols), ct);
        }

        if (_options.Strategy is DuckDbWriteStrategy.Upsert or DuckDbWriteStrategy.Ignore)
        {
            await SetupUpsertStagingAsync(ct);
        }
    }

    private async Task<bool> TableExistsAsync(string? schema, string table, CancellationToken ct)
    {
        using var cmd = _connection!.CreateCommand();
        // DuckDB lowercases unquoted names in information_schema. 
        // Using LOWER() ensures we find the table regardless of whether the test created it 
        // unquoted (lowercase) or quoted (case-preserved).
        if (string.IsNullOrEmpty(schema))
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM information_schema.tables WHERE LOWER(table_name) = LOWER('{table}')";
        }
        else
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM information_schema.tables WHERE LOWER(table_schema) = LOWER('{schema}') AND LOWER(table_name) = LOWER('{table}')";
        }
        
        if (cmd is System.Data.Common.DbCommand dbCmd)
        {
            var res = await dbCmd.ExecuteScalarAsync(ct);
            return Convert.ToInt32(res) > 0;
        }
        else
        {
            var res = cmd.ExecuteScalar();
            return Convert.ToInt32(res) > 0;
        }
    }

    private async Task SetupUpsertStagingAsync(CancellationToken ct)
    {
        var targetInfo = await InspectTargetAsync(ct);
        if (targetInfo?.PrimaryKeyColumns != null) _keyColumns.AddRange(targetInfo.PrimaryKeyColumns);

        if (!string.IsNullOrEmpty(_options.Key))
        {
            _keyColumns.Clear();
            _keyColumns.AddRange(ColumnHelper.ResolveKeyColumns(_options.Key, _columns!));
        }

        if (_keyColumns.Count == 0) throw new InvalidOperationException($"Strategy {_options.Strategy} requires a defined Key.");

        _stagingTable = $"{_options.Table}_stage_{Guid.NewGuid():N}";
        await ExecuteNonQueryAsync($"CREATE TABLE {_stagingTable} AS SELECT * FROM {_quotedTargetTableName} WHERE 1=0", ct);
    }

    private async Task PrepareAppenderAsync(CancellationToken ct)
    {
        var targetInfo = await InspectTargetAsync(ct);
        if (targetInfo != null && targetInfo.Columns.Count > 0)
        {
            _columnMapping = new int[targetInfo.Columns.Count];
            _targetTypes = new Type[targetInfo.Columns.Count];
            _cellConverters = new Func<object?, object?>[targetInfo.Columns.Count];
            for (int i = 0; i < targetInfo.Columns.Count; i++)
            {
                var targetCol = targetInfo.Columns[i];
                _targetTypes[i] = targetCol.InferredClrType ?? typeof(string);
                // F12: build the per-column converter once at init instead of calling
                // ValueConverter.ConvertValue on every cell.
                _cellConverters[i] = CompositeCellJson.Wrap(ColumnConverterFactory.Build(null, _targetTypes[i]));
                var sourceIdx = -1;
                for (int s = 0; s < _columns!.Count; s++)
                {
                    if (string.Equals(_columns[s].Name, targetCol.Name, StringComparison.OrdinalIgnoreCase)) { sourceIdx = s; break; }
                }
                _columnMapping[i] = sourceIdx;
            }
        }
        else
        {
            _columnMapping = Enumerable.Range(0, _columns!.Count).ToArray();
            _targetTypes = _columns.Select(c => c.ClrType).ToArray();
        }

        var appenderTarget = _stagingTable ?? _unquotedTable!;

        if (_hubInfo.IsHub)
        {
            var hubSchema = _stagingTable != null ? null : _unquotedSchema;
            _appender = ((DuckDBConnection)_connection!).CreateAppender(_hubInfo.Alias, hubSchema, appenderTarget);
        }
        else if (_unquotedSchema != null && _unquotedSchema != "main")
        {
            _appender = ((DuckDBConnection)_connection!).CreateAppender(_unquotedSchema, appenderTarget);
        }
        else
        {
            _appender = ((DuckDBConnection)_connection!).CreateAppender(appenderTarget);
        }
    }

    private string BuildQuotedTableName(string? schema, string table)
    {
        var t = _dialect.Quote(table);
        if (string.IsNullOrEmpty(schema)) return t;
        var s = _dialect.Quote(schema);
        return $"{s}.{t}";
    }

    private string GenerateCreateTableSql(string tableName, IEnumerable<PipeColumnInfo> columns, IReadOnlyList<string>? keyColumns = null)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE {tableName} (");
        var list = columns.ToList();
        for (int i = 0; i < list.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            var col = list[i];
            var safeName = _dialect.Quote(col.Name);
            var nativeType = _typeMapper.MapToProviderType(col.ClrType);
            sb.Append($"{safeName} {nativeType}");
        }

        if (keyColumns != null && keyColumns.Count > 0)
        {
            sb.Append($", PRIMARY KEY ({string.Join(", ", keyColumns.Select(k => _dialect.Quote(k)))})");
        }

        sb.Append(")");
        return sb.ToString();
    }

    #endregion

    #region ISchemaInspector, IKeyValidator, ISchemaMigrator

    public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        if (_cachedSchema != null) return _cachedSchema;
        await EnsureConnectionOpenAsync(ct);

        var schemaName = _unquotedSchema;
        var tableName = _unquotedTable ?? _options.Table;

        if (!await TableExistsAsync(schemaName, tableName, ct))
            return new TargetSchemaInfo([], false, null, null, null);

        var columns = new List<TargetColumnInfo>();
        var pkCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        using var cmd = _connection!.CreateCommand();
        var schemaFilter = string.IsNullOrEmpty(schemaName) ? "" : $"AND LOWER(table_schema) = LOWER('{schemaName}')";
        cmd.CommandText = $"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE LOWER(table_name) = LOWER('{tableName}') {schemaFilter} ORDER BY ordinal_position";

        if (cmd is System.Data.Common.DbCommand dbCmd)
        {
            using var reader = await dbCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var name = reader.GetString(0);
                var type = reader.GetString(1);
                var isNullableStr = reader.GetString(2);
                bool notNull = string.Equals(isNullableStr, "NO", StringComparison.OrdinalIgnoreCase);

                columns.Add(new TargetColumnInfo(name, type.ToUpperInvariant(), _typeMapper.MapFromProviderType(type), !notNull, false, false, null));
            }
        }

        var quotedTarget = BuildQuotedTableName(schemaName, tableName);
        long? rowCount = null;
        try
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM {quotedTarget}";
            if (cmd is System.Data.Common.DbCommand dbCmdRow)
            {
                var res = await dbCmdRow.ExecuteScalarAsync(ct);
                rowCount = Convert.ToInt64(res);
            }
            else
            {
                var res = cmd.ExecuteScalar();
                rowCount = Convert.ToInt64(res);
            }
        }
        catch { /* Fallback if count fails */ }

        _cachedSchema = new TargetSchemaInfo(columns, true, rowCount, null, pkCols.Count > 0 ? pkCols.ToList() : null);
        return _cachedSchema;
    }


    public string? GetWriteStrategy() => _options.Strategy.ToString();
    public IReadOnlyList<string>? GetRequestedPrimaryKeys() => KeyHelper.ParseKeySpec(_options.Key);
    public bool RequiresPrimaryKey() => _options.Strategy is DuckDbWriteStrategy.Upsert or DuckDbWriteStrategy.Ignore;

    public async ValueTask MigrateSchemaAsync(DtPipe.Core.Validation.SchemaCompatibilityReport report, CancellationToken ct)
    {
        var missingColumns = report.Columns
            .Where(c => c.Status == DtPipe.Core.Validation.CompatibilityStatus.MissingInTarget && c.SourceColumn != null)
            .Select(c => c.SourceColumn!)
            .ToList();

        if (missingColumns.Count == 0) return;

        await EnsureConnectionOpenAsync(ct);
        foreach (var col in missingColumns)
        {
            var safeName = _dialect.Quote(col.Name);
            var nativeType = _typeMapper.MapToProviderType(col.ClrType);
            await ExecuteNonQueryAsync($"ALTER TABLE {_quotedTargetTableName} ADD COLUMN {safeName} {nativeType}", ct);
        }

        InvalidateSchemaCache();
    }
    #endregion
}
