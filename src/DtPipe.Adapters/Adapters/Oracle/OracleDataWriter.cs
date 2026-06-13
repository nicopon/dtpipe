using Oracle.ManagedDataAccess.Client;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using System.Data;
using System.Text;
using DtPipe.Adapters.Oracle;
using Microsoft.Extensions.Logging;
using DtPipe.Core.Helpers;

public sealed class OracleDataWriter : BaseSqlDataWriter
{
    private readonly OracleWriterOptions _options;
    private readonly ILogger<OracleDataWriter> _logger;
    private readonly ITypeMapper _typeMapper;
    private OracleBulkCopy? _bulkCopy;
    private OracleCommand? _insertCommand;
    private OracleParameter[]? _insertParameters;
    private OracleCommand? _mergeCommand;
    private OracleParameter[]? _mergeParameters;
    private Func<object?, object?>[]? _converters;

    // Helper property to avoid casting _connection everywhere
    private OracleConnection OracleConnection => (OracleConnection)_connection!;

    // Maps column names to target DB types (e.g. "ID" -> OracleDbType.Raw)
    private Dictionary<string, OracleDbType>? _targetColumnTypes;

    private List<string> _keyColumns = new();
    private List<int> _keyIndices = new();

    public override ISqlDialect Dialect => new DtPipe.Core.Dialects.OracleDialect();

	public override bool RequiresTargetInspection => _options.Strategy != OracleWriteStrategy.Recreate;

	protected override ITypeMapper GetTypeMapper() => _typeMapper;

    public OracleDataWriter(string connectionString, OracleWriterOptions options, ILogger<OracleDataWriter> logger, ITypeMapper typeMapper)
        : base(connectionString)
    {
        _options = options;
        _logger = logger;
        _typeMapper = typeMapper;
        if(_logger.IsEnabled(LogLevel.Debug)) _logger.LogDebug("OracleDataWriter created");
    }

    #region BaseSqlDataWriter Implementation

    protected override async Task<TargetSchemaInfo?> InspectTargetInternalAsync(CancellationToken ct = default)
    {
        return await new OracleSchemaInspector(_connectionString, _options.Table, _logger).InspectTargetAsync(ct);
    }

    protected override IDbConnection CreateConnection(string connectionString)
        => new OracleConnection(connectionString);

    protected override async Task<(string Schema, string Table)> ResolveTargetTableAsync(CancellationToken ct)
    {
        try
        {
            var resolved = await OracleSchemaInspector.ResolveTargetTableAsync(OracleConnection, _options.Table, ct);
            return (resolved.Schema, resolved.Table);
        }
        catch (OracleException ex) when (ex.Number == 6550 || ex.Number == 6564 || ex.Message.Contains("ORA-06550") || ex.Message.Contains("ORA-06564"))
        {
            // Object doesn't exist yet - return raw input
            return ("", _options.Table);
        }
    }

    protected override async Task<TargetSchemaInfo?> ApplyWriteStrategyAsync(string resolvedSchema, string resolvedTable, CancellationToken ct)
    {
        TargetSchemaInfo? result = null;
        switch (_options.Strategy)
        {
            case OracleWriteStrategy.Recreate:
                result = await base.ApplyRecreateStrategyAsync(ct);
                break;
            case OracleWriteStrategy.Truncate:
                result = await base.ApplyTruncateStrategyAsync(ct);
                break;
            case OracleWriteStrategy.Append:
                result = await base.ApplyAppendStrategyAsync(ct);
                break;
            case OracleWriteStrategy.DeleteThenInsert:
                result = await base.ApplyDeleteThenInsertStrategyAsync(ct);
                break;
            case OracleWriteStrategy.Upsert:
            case OracleWriteStrategy.Ignore:
                await ApplyUpsertIgnoreStrategyAsync(ct);
                break;
        }

        await SyncFromTargetAsync(ct);
        return result;
    }

    protected override async ValueTask OnInitializedAsync(CancellationToken ct)
    {
        InitializeCommands();
        await ValueTask.CompletedTask;
    }

    protected override async Task ExecuteDropTableSafeAsync(CancellationToken ct)
    {
        try
        {
            await ExecuteNonQueryAsync(GetDropTableSql(_quotedTargetTableName), ct);
        }
        catch (OracleException ex) when (ex.Number == 942) { /* ORA-00942: table or view does not exist */ }
    }

    private async Task ApplyUpsertIgnoreStrategyAsync(CancellationToken ct)
    {
        // 1. Sync columns from target
        var targetInfo = await InspectTargetAsync(ct);
        if (targetInfo != null)
        {
            SyncColumnsFromIntrospection(targetInfo);
        }

        // 2. Resolve Keys using base class logic
        await base.ResolveKeysAsync(_keyColumns, ct);

        // 3. Resolve key indices (Oracle-specific)
        foreach (var key in _keyColumns)
        {
            var idx = -1;
            for (int i = 0; i < _columns!.Count; i++)
            {
                if (string.Equals(_columns[i].Name, key, StringComparison.OrdinalIgnoreCase))
                {
                    idx = i;
                    break;
                }
            }
            if (idx == -1) throw new InvalidOperationException($"Key column '{key}' not found in source columns.");
            _keyIndices.Add(idx);
        }
    }

    private async Task SyncFromTargetAsync(CancellationToken ct)
    {
        // Introspect target types (Fix for ORA-00932 BLOB/RAW mismatch)
        try
        {
            var schemaInfo = await InspectTargetAsync(ct);
            if (schemaInfo != null)
            {
                SyncColumnsFromIntrospection(schemaInfo);
                _targetColumnTypes = new Dictionary<string, OracleDbType>(StringComparer.OrdinalIgnoreCase);
                foreach (var col in schemaInfo.Columns)
                {
                    var dbType = OracleTypeConverter.MapNativeTypeToOracleDbType(col.NativeType);
                    if (dbType.HasValue)
                    {
                        _targetColumnTypes[col.Name] = dbType.Value;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            if (_logger.IsEnabled(LogLevel.Warning))
                _logger.LogWarning(ex, "Failed to inspect target schema types. Parameter binding will rely on Source types.");
        }
    }

	protected override string GetTruncateTableSql(string tableName)
        => $"TRUNCATE TABLE {tableName}";

    protected override string GetDropTableSql(string tableName)
        => $"DROP TABLE {tableName}";

    protected override string AddColumnKeyword => "ADD";

    public override async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_columns is null) throw new InvalidOperationException("Not initialized");

        if (_options.Strategy == OracleWriteStrategy.Upsert || _options.Strategy == OracleWriteStrategy.Ignore)
        {
             if (_mergeCommand == null || _mergeParameters == null) throw new InvalidOperationException("Merge command not initialized");

             if(_logger.IsEnabled(LogLevel.Debug))
                 _logger.LogDebug("Executing Merge for batch of {Count} rows", rows.Count);
             await BindAndExecuteAsync(_mergeCommand, _mergeParameters, rows, ct);
             return;
        }

        var effectiveMode = _options.InsertMode ?? OracleInsertMode.Standard;

        if (effectiveMode == OracleInsertMode.Standard || effectiveMode == OracleInsertMode.Append)
        {
            // Standard/Append INSERT using Array Binding
            if (_insertCommand == null || _insertParameters == null)
                 throw new InvalidOperationException("Insert command not initialized");

            int rowCount = rows.Count;
            _insertCommand.ArrayBindCount = rowCount;
            TransposeAndBindParameters(_insertParameters, rows);

            try
            {
                await _insertCommand.ExecuteNonQueryAsync(ct);
                if(_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug("Inserted {Count} rows via Array Binding ({Mode})...", rowCount, effectiveMode);
            }
            catch (Exception ex)
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                    _logger.LogWarning(ex, "Array Binding Insert failed. Starting in-depth analysis of the batch...");

                var analysis = await BatchFailureAnalyzer.AnalyzeAsync(this, rows, _columns, ct);
                if (!string.IsNullOrEmpty(analysis))
                {
                    throw new InvalidOperationException($"Array Binding Insert Failed with detailed analysis:\n{analysis}{Environment.NewLine}SQL: {_insertCommand.CommandText}", ex);
                }
                throw new InvalidOperationException($"Array Binding Insert Failed.{Environment.NewLine}SQL: {_insertCommand.CommandText}{Environment.NewLine}Error: {ex.Message}", ex);
            }
        }
        else
        {
            if (_bulkCopy == null) throw new InvalidOperationException("BulkCopy not initialized");

            _bulkCopy.BatchSize = rows.Count;
            if(_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Starting BulkCopy for batch of {Count} rows into {Table}", rows.Count, _quotedTargetTableName);
            var watch = System.Diagnostics.Stopwatch.StartNew();

            // Use IDataReader wrapper for performance
            using var dataReader = new ObjectArrayDataReader(_columns, rows);
            try
            {
                await Task.Run(() => _bulkCopy.WriteToServer(dataReader), ct);
            }
            catch (Exception ex)
            {
                if(_logger.IsEnabled(LogLevel.Warning))
                    _logger.LogWarning(ex, "OracleBulkCopy failed. Starting in-depth analysis of the batch...");
                var analysis = await BatchFailureAnalyzer.AnalyzeAsync(this, rows, _columns, ct);
                if (!string.IsNullOrEmpty(analysis))
                {
                    throw new InvalidOperationException($"Bulk Copy Failed with detailed analysis:\n{analysis}", ex);
                }
                throw; // Rethrow original if we couldn't find the specific culprit
            }
            watch.Stop();
            if(_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("BulkCopy finished in {ElapsedMs}ms", watch.ElapsedMilliseconds);
        }
    }

    private async Task BindAndExecuteAsync(OracleCommand cmd, OracleParameter[] paramsArray, IReadOnlyList<object?[]> rows, CancellationToken ct)
    {
        int rowCount = rows.Count;
        cmd.ArrayBindCount = rowCount;
        TransposeAndBindParameters(paramsArray, rows);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Transposes row-oriented data into column-oriented Oracle arrays and binds them to parameters.
    /// Used by both Insert (Array Binding) and Merge (MERGE SQL) paths.
    /// </summary>
    private void TransposeAndBindParameters(OracleParameter[] paramsArray, IReadOnlyList<object?[]> rows)
    {
        int rowCount = rows.Count;

        for (int colIndex = 0; colIndex < _columns!.Count; colIndex++)
        {
            var colValues = new object?[rowCount];
            var colType = _columns[colIndex].ClrType;
            bool isBool = colType == typeof(bool);

            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
            {
                var val = rows[rowIndex][colIndex];
                if (val is null || val == DBNull.Value)
                {
                    colValues[rowIndex] = DBNull.Value;
                }
                else if (isBool)
                {
                    colValues[rowIndex] = ((bool)val) ? 1 : 0;
                }
                else
                {
                    colValues[rowIndex] = _converters![colIndex](val);
                }
            }
            paramsArray[colIndex].Value = colValues;
        }
    }

    public override async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        await ValueTask.CompletedTask;
    }


    protected override async ValueTask DisposeResourcesAsync()
    {
        if (_bulkCopy != null)
        {
            ((IDisposable)_bulkCopy).Dispose();
            _bulkCopy = null;
        }

        if (_insertCommand != null)
        {
            _insertCommand.Dispose();
            _insertCommand = null;
        }

        if (_mergeCommand != null)
        {
            _mergeCommand.Dispose();
            _mergeCommand = null;
        }

        await ValueTask.CompletedTask;
    }

    public override string? GetWriteStrategy() => _options.Strategy.ToString();

    protected override string? GetRequestedKeySpec() => _options.Key;

    public override bool RequiresPrimaryKey()
    {
        return _options.Strategy is
            OracleWriteStrategy.Upsert or
            OracleWriteStrategy.Ignore;
    }



    private void InitializeCommands()
    {
        _converters = new Func<object?, object?>[_columns!.Count];
        for (int i = 0; i < _columns.Count; i++)
        {
            _converters[i] = ColumnConverterFactory.Build(_columns[i].ClrType, _columns[i].ClrType);
        }

        if (_options.Strategy == OracleWriteStrategy.Upsert || _options.Strategy == OracleWriteStrategy.Ignore)
        {
             bool isUpsert = _options.Strategy == OracleWriteStrategy.Upsert;
             var (mergeSql, types) = OracleSqlBuilder.BuildMergeSql(
                 _quotedTargetTableName,
                 _columns!,
                 _keyColumns,
                 Dialect,
                 isUpsert,
                 _options.DateTimeMapping);

             if(_logger.IsEnabled(LogLevel.Debug))
                 _logger.LogDebug("Generated MERGE SQL: {Sql}", mergeSql);

             _mergeCommand = OracleConnection.CreateCommand();
             _mergeCommand.BindByName = true;
             _mergeCommand.CommandText = mergeSql;

             _mergeParameters = new OracleParameter[_columns!.Count];
             for(int i=0; i<_columns.Count; i++)
             {
                 var p = _mergeCommand.CreateParameter();
                 p.ParameterName = $"v{i}";

                 // Override type if target is known (e.g. RAW vs BLOB)
                 if (_targetColumnTypes != null && _targetColumnTypes.TryGetValue(_columns[i].Name, out var targetType))
                 {
                     p.OracleDbType = targetType;
                 }
                 else
                 {
                     p.OracleDbType = types[i];
                 }

                 _mergeCommand.Parameters.Add(p);
                 _mergeParameters[i] = p;
             }
        }
        else if (_options.InsertMode == OracleInsertMode.Bulk)
        {
            _bulkCopy = new OracleBulkCopy(OracleConnection);

            // IMPORTANT: OracleBulkCopy requires SEPARATE properties for schema and table.
            // It does NOT accept "SCHEMA.TABLE" in DestinationTableName.

            if (_quotedTargetTableName.Contains('.'))
            {
                var parts = _quotedTargetTableName.Split('.');
                // Remove quotes if present (e.g., "SYSTEM"."MyTable" -> SYSTEM, MyTable)
                var schema = parts[0].Trim('"');
                var table = parts[1].Trim('"');

                _bulkCopy.DestinationSchemaName = schema;
                _bulkCopy.DestinationTableName = table;
            }
            else
            {
                // No schema prefix, use table name only (defaults to connection user's schema)
                _bulkCopy.DestinationTableName = _quotedTargetTableName.Trim('"');
            }

            _bulkCopy.BulkCopyTimeout = 0;
            foreach (var col in _columns!)
            {
                // Columns are already globally normalized in InitializeAsync
                _bulkCopy.ColumnMappings.Add(col.Name, SqlIdentifierHelper.GetSafeIdentifier(Dialect, col));
            }
        }
        else
        {
            bool useAppendHint = _options.InsertMode == OracleInsertMode.Append;
            var (insertSql, types) = OracleSqlBuilder.BuildInsertSql(
                _quotedTargetTableName,
                _columns!,
                Dialect,
                useAppendHint,
                _options.DateTimeMapping);

            if(_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Generated Insert SQL: {Sql}", insertSql);

            _insertCommand = OracleConnection.CreateCommand();
            _insertCommand.BindByName = true;
            _insertCommand.CommandText = insertSql;

            _insertParameters = new OracleParameter[_columns!.Count];
            for (int i = 0; i < _columns.Count; i++)
            {
                _insertParameters[i] = _insertCommand.CreateParameter();
                _insertParameters[i].ParameterName = $"v{i}";

                // Override type if target is known
                if (_targetColumnTypes != null && _targetColumnTypes.TryGetValue(_columns[i].Name, out var targetType))
                {
                    _insertParameters[i].OracleDbType = targetType;
                }
                else
                {
                    _insertParameters[i].OracleDbType = types[i];
                }

                _insertCommand.Parameters.Add(_insertParameters[i]);
            }
        }
    }

    #endregion


}
