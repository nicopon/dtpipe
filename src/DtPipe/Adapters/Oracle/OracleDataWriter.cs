using Oracle.ManagedDataAccess.Client;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using System.Data;
using System.Text;
using DtPipe.Adapters.Oracle;
using Microsoft.Extensions.Logging;
using DtPipe.Core.Helpers;

public sealed class OracleDataWriter : IDataWriter, ISchemaInspector, IKeyValidator
{
    private readonly string _connectionString;
    private readonly OracleConnection _connection;
    private readonly OracleWriterOptions _options;
    private IReadOnlyList<PipeColumnInfo>? _columns;
    private readonly ILogger<OracleDataWriter> _logger;
    private readonly ITypeMapper _typeMapper;
    private OracleBulkCopy? _bulkCopy;
    private OracleCommand? _insertCommand;
    private OracleParameter[]? _insertParameters;

    // Maps column names to target DB types (e.g. "ID" -> OracleDbType.Raw)
    private Dictionary<string, OracleDbType>? _targetColumnTypes;

    private List<string> _keyColumns = new();
    private List<int> _keyIndices = new();

    private readonly ISqlDialect _dialect = new DtPipe.Core.Dialects.OracleDialect();
    public ISqlDialect Dialect => _dialect;

    public OracleDataWriter(string connectionString, OracleWriterOptions options, ILogger<OracleDataWriter> logger, ITypeMapper typeMapper)
    {
        _connectionString = connectionString;
        _options = options;
        _logger = logger;
        _typeMapper = typeMapper;
        _connection = new OracleConnection(connectionString);
        if(_logger.IsEnabled(LogLevel.Debug)) _logger.LogDebug("OracleDataWriter created");
    }

    private string _targetTableName = "";

    #region ISchemaInspector Implementation

    public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        return await new OracleSchemaInspector(_connectionString, _options.Table, _logger).InspectTargetAsync(ct);
    }

    #endregion

    public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        _targetTableName = _options.Table;

        // Create normalized list of columns to ensure consistency across DDL and DML
        var normalizedColumns = new List<PipeColumnInfo>(columns.Count);
        foreach (var col in columns)
        {
            // Strip BOM and whitespace (common in CSV headers)
            var cleanName = col.Name.Trim('\uFEFF', ' ', '\t', '\r', '\n');

            if (col.IsCaseSensitive)
            {
                normalizedColumns.Add(col with { Name = cleanName });
            }
            else
            {
                // Normalize to Oracle default (UPPERCASE)
                normalizedColumns.Add(col with { Name = _dialect.Normalize(cleanName) });
            }
        }
        _columns = normalizedColumns;

        if(_logger.IsEnabled(LogLevel.Information))
            _logger.LogInformation("Initializing Oracle Writer for table {Table} (WriteStrategy={Strategy})", _targetTableName, _options.Strategy);

        if (_connection.State != ConnectionState.Open)
        {
            await _connection.OpenAsync(ct);
        }

        if (_options.Strategy == OracleWriteStrategy.Recreate)
        {
            // 0. Introspect existing table to preserve native types
            TargetSchemaInfo? existingSchema = null;
            try
            {
                var resolved = await OracleSchemaInspector.ResolveTargetTableAsync(_connection, _options.Table, ct);
                if (!string.IsNullOrEmpty(resolved.Table))
                {
                    var safeSchema = OracleSchemaInspector.GetSmartQuotedIdentifier(resolved.Schema);
                    var safeTable = OracleSchemaInspector.GetSmartQuotedIdentifier(resolved.Table);
                    _targetTableName = $"{safeSchema}.{safeTable}";

                    existingSchema = await InspectTargetAsync(ct);
                    if (existingSchema?.Exists == true)
                    {
                        if(_logger.IsEnabled(LogLevel.Information))
                            _logger.LogInformation("Table {Table} exists. Preserving native schema for recreation.", _targetTableName);
                    }
                }
            }
            catch
            {
                if(_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug("Target table {Table} could not be resolved. Proceeding with default creation.", _options.Table);
            }

            // 1. Drop existing table
            try {
                using var dropCmd = _connection.CreateCommand();
                var sql = $"DROP TABLE {_targetTableName}";
                dropCmd.CommandText = sql;
                await dropCmd.ExecuteNonQueryAsync(ct);
                if(_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation("Dropped table {Table} (Recreate Strategy)", _targetTableName);
            }
            catch (OracleException ex) when (ex.Number == 942) { /* ORA-00942: table or view does not exist */ }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Recreate Drop failed. SQL: DROP TABLE {_targetTableName}. Error: {ex.Message}", ex);
            }

            // 2. Create new table
            string createTableSql;
            if (existingSchema?.Exists == true)
            {
                createTableSql = OracleSqlBuilder.BuildCreateTableFromIntrospection(_targetTableName, existingSchema, _dialect);
            }
            else
            {
                createTableSql = BuildCreateTableSql(_targetTableName, normalizedColumns);
            }

            try
            {
                using var cmd = _connection.CreateCommand();
                cmd.CommandText = createTableSql;
                await cmd.ExecuteNonQueryAsync(ct);
                if(_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation("Created table {Table}", _targetTableName);

                // Sync columns metadata from introspection to ensure future DML (INSERT/MERGE) matches exact case/quotes
                if (existingSchema != null)
                {
                    SyncColumnsFromIntrospection(existingSchema);
                }
            }
            catch (Exception ex)
            {
                 throw new InvalidOperationException($"Create Table failed. SQL: {createTableSql}{Environment.NewLine}Error: {ex.Message}", ex);
            }
        }
        else
        {
            try
            {
                var resolved = await OracleSchemaInspector.ResolveTargetTableAsync(_connection, _options.Table, ct);

                var safeSchema = OracleSchemaInspector.GetSmartQuotedIdentifier(resolved.Schema);
                var safeTable = OracleSchemaInspector.GetSmartQuotedIdentifier(resolved.Table);

                _targetTableName = $"{safeSchema}.{safeTable}";

                if(_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation("Resolved table {Input} to {Resolved}", _options.Table, _targetTableName);
            }
            catch (OracleException ex) when (ex.Number == 6550 || ex.Message.Contains("ORA-06550"))
            {
                // ORA-06550: PL/SQL compilation error often means the object does not exist or access denied in NAME_RESOLVE
                 throw new InvalidOperationException($"Table {_options.Table} could not be resolved or does not exist. (Oracle NAME_RESOLVE failed). Strategy: {_options.Strategy}", ex);
            }
            catch (Exception ex)
            {
                 throw new InvalidOperationException($"Failed to resolve table {_options.Table}. Error: {ex.Message}", ex);
            }

            // Introspect target types (Fix for ORA-00932 BLOB/RAW mismatch)
            try
            {
                var schemaInfo = await InspectTargetAsync(ct);
                if (schemaInfo != null)
                {
                    SyncColumnsFromIntrospection(schemaInfo);
                    _targetColumnTypes = new Dictionary<string, OracleDbType>(StringComparer.OrdinalIgnoreCase);
                    foreach(var col in schemaInfo.Columns)
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
                if(_logger.IsEnabled(LogLevel.Warning))
                    _logger.LogWarning(ex, "Failed to inspect target schema types. Parameter binding will rely on Source types.");
            }

            // Handle cleanup for strategies that require it
            if (_options.Strategy == OracleWriteStrategy.DeleteThenInsert)
            {
                try {
                    using var deleteCmd = _connection.CreateCommand();
                    var sql = $"DELETE FROM {_targetTableName}";
                    deleteCmd.CommandText = sql;
                    await deleteCmd.ExecuteNonQueryAsync(ct);
                    if(_logger.IsEnabled(LogLevel.Information))
                        _logger.LogInformation("Deleted existing rows from table {Table}", _targetTableName);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Delete failed. SQL: DELETE FROM {_targetTableName}. Error: {ex.Message}", ex);
                }
            }
            else if (_options.Strategy == OracleWriteStrategy.Truncate)
            {
                 try {
                    using var truncCmd = _connection.CreateCommand();
                    var sql = $"TRUNCATE TABLE {_targetTableName}";
                    truncCmd.CommandText = sql;
                    await truncCmd.ExecuteNonQueryAsync(ct);
                    if(_logger.IsEnabled(LogLevel.Information))
                        _logger.LogInformation("Truncated table {Table}", _targetTableName);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Truncate failed. SQL: TRUNCATE TABLE {_targetTableName}. Error: {ex.Message}", ex);
                }
            }
        }

        if (_options.Strategy == OracleWriteStrategy.Upsert || _options.Strategy == OracleWriteStrategy.Ignore)
        {
             // 1. Resolve Keys
            var targetInfo = await InspectTargetAsync(ct);
            if (targetInfo != null)
            {
                SyncColumnsFromIntrospection(targetInfo);
                if (targetInfo.PrimaryKeyColumns != null)
                {
                    _keyColumns.AddRange(targetInfo.PrimaryKeyColumns);
                }
            }

            if (_keyColumns.Count == 0 && !string.IsNullOrEmpty(_options.Key))
            {
                 _keyColumns.AddRange(ColumnHelper.ResolveKeyColumns(_options.Key, _columns));
            }

            if (_keyColumns.Count == 0)
            {
                 throw new InvalidOperationException($"Strategy {_options.Strategy} requires a Primary Key. None detected.");
            }

            // Resolve Indices
            foreach(var key in _keyColumns)
            {
                var idx = -1;
                for(int i=0; i<columns.Count; i++)
                {
                    if (string.Equals(columns[i].Name, key, StringComparison.OrdinalIgnoreCase))
                    {
                        idx = i;
                        break;
                    }
                }
                if (idx == -1) throw new InvalidOperationException($"Key column '{key}' not found in source columns.");
                _keyIndices.Add(idx);
            }
        }

        // Initialize reusable objects
        InitializeCommands();
    }

    private OracleCommand? _mergeCommand;
    private OracleParameter[]? _mergeParameters;

    private void InitializeCommands()
    {
        if (_options.Strategy == OracleWriteStrategy.Upsert || _options.Strategy == OracleWriteStrategy.Ignore)
        {
             bool isUpsert = _options.Strategy == OracleWriteStrategy.Upsert;
             var (mergeSql, types) = OracleSqlBuilder.BuildMergeSql(
                 _targetTableName,
                 _columns!,
                 _keyColumns,
                 _dialect,
                 isUpsert,
                 _options.DateTimeMapping);

             if(_logger.IsEnabled(LogLevel.Debug))
                 _logger.LogDebug("Generated MERGE SQL: {Sql}", mergeSql);

             _mergeCommand = _connection.CreateCommand();
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
            _bulkCopy = new OracleBulkCopy(_connection);

            // IMPORTANT: OracleBulkCopy requires SEPARATE properties for schema and table.
            // It does NOT accept "SCHEMA.TABLE" in DestinationTableName.

            if (_targetTableName.Contains('.'))
            {
                var parts = _targetTableName.Split('.');
                // Remove quotes if present (e.g., "SYSTEM"."MyTable" -> SYSTEM, MyTable)
                var schema = parts[0].Trim('"');
                var table = parts[1].Trim('"');

                _bulkCopy.DestinationSchemaName = schema;
                _bulkCopy.DestinationTableName = table;
            }
            else
            {
                // No schema prefix, use table name only (defaults to connection user's schema)
                _bulkCopy.DestinationTableName = _targetTableName.Trim('"');
            }

            _bulkCopy.BulkCopyTimeout = 0;
            foreach (var col in _columns!)
            {
                // Columns are already globally normalized in InitializeAsync
                _bulkCopy.ColumnMappings.Add(col.Name, SqlIdentifierHelper.GetSafeIdentifier(_dialect, col));
            }
        }
        else
        {
            bool useAppendHint = _options.InsertMode == OracleInsertMode.Append;
            var (insertSql, types) = OracleSqlBuilder.BuildInsertSql(
                _targetTableName,
                _columns!,
                _dialect,
                useAppendHint,
                _options.DateTimeMapping);

            if(_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Generated Insert SQL: {Sql}", insertSql);

            _insertCommand = _connection.CreateCommand();
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

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
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

            // Transpose rows to columns with type safety
            for (int colIndex = 0; colIndex < _columns.Count; colIndex++)
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
                        var converted = DtPipe.Core.Helpers.ValueConverter.ConvertValue(val, colType);
                        if (rowIndex == 0 && _logger.IsEnabled(LogLevel.Debug))
                        {
                            _logger.LogDebug("Column {Col} (Type: {Type}) value: {Val} (Actual Type: {ActualType})",
                                _columns[colIndex].Name, colType.Name, converted, converted?.GetType().Name);
                        }
                        colValues[rowIndex] = converted;
                    }
                }
                _insertParameters[colIndex].Value = colValues;
            }

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
                _logger.LogDebug("Starting BulkCopy for batch of {Count} rows into {Table}", rows.Count, _targetTableName);
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

            // Transpose rows to columns with type safety
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
                        colValues[rowIndex] = DtPipe.Core.Helpers.ValueConverter.ConvertValue(val, colType);
                    }
                }
                paramsArray[colIndex].Value = colValues;
            }

            await cmd.ExecuteNonQueryAsync(ct);
    }

    public ValueTask CompleteAsync(CancellationToken ct = default)
    {
        return ValueTask.CompletedTask;
    }
    public async ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
    {
        if (_connection.State != ConnectionState.Open)
        {
            await _connection.OpenAsync(ct);
        }

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = command;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask DisposeAsync()
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

        await _connection.DisposeAsync();
    }



    /// <summary>
    /// Builds CREATE TABLE DDL from source column info.
    /// </summary>
    /// <remarks>
    /// Types are mapped from CLR types. Use existing schema or manage DDL separately
    /// if exact structure (precision/scale) must be preserved.
    /// </remarks>
    private string BuildCreateTableSql(string tableName, IReadOnlyList<PipeColumnInfo> columns)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE {tableName} (");

        for (int i = 0; i < columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            // Columns are passed from _columns which is already globally normalized
            sb.Append($"{SqlIdentifierHelper.GetSafeIdentifier(_dialect, columns[i])} {_typeMapper.MapToProviderType(columns[i].ClrType)}");
        }

        if (!string.IsNullOrEmpty(_options.Key))
        {
             var resolvedKeys = ColumnHelper.ResolveKeyColumns(_options.Key, columns.ToList());
             var safeKeys = resolvedKeys.Select(keyName =>
             {
                 var col = columns.First(c => c.Name == keyName);
                 return SqlIdentifierHelper.GetSafeIdentifier(_dialect, col);
             }).ToList();
             sb.Append($", PRIMARY KEY ({string.Join(", ", safeKeys)})");
        }

        sb.Append(")");
        return sb.ToString();
    }



    // IKeyValidator implementation

    public string? GetWriteStrategy()
    {
        return _options.Strategy.ToString();
    }

    public IReadOnlyList<string>? GetRequestedPrimaryKeys()
    {
        if (string.IsNullOrEmpty(_options.Key))
            return null;

        // Return the RAW user input (not yet resolved)
        return _options.Key.Split(',')
            .Select(k => k.Trim())
            .Where(k => !string.IsNullOrEmpty(k))
            .ToList();
    }

    public bool RequiresPrimaryKey()
    {
        return _options.Strategy is
            OracleWriteStrategy.Upsert or
            OracleWriteStrategy.Ignore;
    }

    /// <summary>
    /// Synchronizes source column metadata with the target schema information.
    /// Updates Name (to match exact casing in target DB), IsCaseSensitive flag,
    /// and ClrType (to enable correct type conversion, e.g. string → DateTime).
    /// Columns not found in the target schema are kept unchanged.
    /// </summary>
    private void SyncColumnsFromIntrospection(TargetSchemaInfo targetSchema)
    {
        if (_columns == null) return;

        var synced = new List<PipeColumnInfo>(_columns.Count);
        foreach (var col in _columns)
        {
            var targetCol = targetSchema.Columns.FirstOrDefault(
                tc => tc.Name.Equals(col.Name, StringComparison.OrdinalIgnoreCase)
            );

            if (targetCol != null)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug("Syncing column {Col}: Name {Old} -> {New}, ClrType {OldType} -> {NewType}, IsCaseSensitive {OldCS} -> {NewCS}",
                        col.Name, col.Name, targetCol.Name, col.ClrType.Name, (targetCol.InferredClrType ?? col.ClrType).Name, col.IsCaseSensitive, targetCol.IsCaseSensitive);

                synced.Add(col with
                {
                    Name = targetCol.Name,
                    IsCaseSensitive = targetCol.IsCaseSensitive,
                    ClrType = targetCol.InferredClrType ?? col.ClrType
                });
            }
            else
            {
                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation("Column {Col} was NOT found in target introspection. It will remain mapped to {Type} (IsCaseSensitive: {CS}).", col.Name, col.ClrType.Name, col.IsCaseSensitive);
                synced.Add(col);
            }
        }
        _columns = synced;
    }
}
