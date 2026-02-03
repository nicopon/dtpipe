using Oracle.ManagedDataAccess.Client;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using System.Data;
using System.Text;
using DtPipe.Adapters.Oracle;
using Microsoft.Extensions.Logging;
using DtPipe.Core.Helpers;

public sealed class OracleDataWriter : IDataWriter, ISchemaInspector
{
    private readonly string _connectionString;
    private readonly OracleConnection _connection;
    private readonly OracleWriterOptions _options;
    private IReadOnlyList<ColumnInfo>? _columns;
    private readonly ILogger<OracleDataWriter> _logger;
    private OracleBulkCopy? _bulkCopy;
    private OracleCommand? _insertCommand;
    private OracleParameter[]? _insertParameters;

    // Map column names to actual DB types (e.g. "ID" -> OracleDbType.Raw)
    private Dictionary<string, OracleDbType>? _targetColumnTypes;

    private List<string> _keyColumns = new();
    private List<int> _keyIndices = new();
    
    private readonly ISqlDialect _dialect = new DtPipe.Core.Dialects.OracleDialect();
    public ISqlDialect Dialect => _dialect;

    public OracleDataWriter(string connectionString, OracleWriterOptions options, ILogger<OracleDataWriter> logger)
    {
        _connectionString = connectionString;
        _options = options;
        _logger = logger;
        _connection = new OracleConnection(connectionString);
        _logger.LogDebug("OracleDataWriter created");
    }

    private string _targetTableName = "";

    #region ISchemaInspector Implementation

    public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        _logger.LogDebug("Starting target schema inspection for table {Table}", _targetTableName);
        await using var connection = new OracleConnection(_connectionString);
        await connection.OpenAsync(ct);

        var (owner, tableName) = ParseTableName(_targetTableName);
        bool hasOwner = !string.IsNullOrEmpty(owner);

        // Helper to switch between ALL_ (with owner check) and USER_ (implicit owner) views
        string GetView(string viewSuffix) => hasOwner ? $"ALL_{viewSuffix}" : $"USER_{viewSuffix}";
        
        void AddOwnerParam(OracleCommand cmd)
        {
            if (hasOwner) cmd.Parameters.Add(new OracleParameter("p_owner", owner));
        }

        // Check if table exists and get row count
        var existsView = GetView("TABLES");
        var existsSql = hasOwner
            ? $"SELECT num_rows FROM {existsView} WHERE owner = :p_owner AND table_name = :p_table"
            : $"SELECT num_rows FROM {existsView} WHERE table_name = :p_table";
        
        using var existsCmd = connection.CreateCommand();
        existsCmd.BindByName = true;
        existsCmd.CommandText = existsSql;
        AddOwnerParam(existsCmd);
        existsCmd.Parameters.Add(new OracleParameter("p_table", tableName));
        
        _logger.LogDebug("Checking table existence with SQL: {Sql}", existsSql);
        var result = await existsCmd.ExecuteScalarAsync(ct);
        if (result == null)
        {
            return new TargetSchemaInfo([], false, null, null, null);
        }
        
        var rowCount = result == DBNull.Value ? (long?)null : Convert.ToInt64(result);

        // Get columns
        var colView = GetView("TAB_COLUMNS");
        var columnsSql = hasOwner
            ? $@"SELECT 
                column_name, data_type, nullable, data_length, data_precision, data_scale, char_length
            FROM {colView} 
            WHERE owner = :p_owner AND table_name = :p_table
            ORDER BY column_id"
            : $@"SELECT 
                column_name, data_type, nullable, data_length, data_precision, data_scale, char_length
            FROM {colView} 
            WHERE table_name = :p_table
            ORDER BY column_id";
        
        using var columnsCmd = connection.CreateCommand();
        columnsCmd.BindByName = true;
        columnsCmd.CommandText = columnsSql;
        AddOwnerParam(columnsCmd);
        columnsCmd.Parameters.Add(new OracleParameter("p_table", tableName));

        // Get primary key columns
        var consView = GetView("CONSTRAINTS");
        var consColView = GetView("CONS_COLUMNS");
        
        string pkSql;
        if (hasOwner)
        {
            pkSql = $@"
            SELECT cols.column_name
            FROM {consView} cons
            JOIN {consColView} cols ON cons.constraint_name = cols.constraint_name 
                AND cons.owner = cols.owner
            WHERE cons.constraint_type = 'P' 
              AND cons.owner = :p_owner 
              AND cons.table_name = :p_table";
        }
        else
        {
            pkSql = $@"
            SELECT cols.column_name
            FROM {consView} cons
            JOIN {consColView} cols ON cons.constraint_name = cols.constraint_name 
            WHERE cons.constraint_type = 'P' 
              AND cons.table_name = :p_table";
        }
        
        var pkColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var pkCmd = connection.CreateCommand())
        {
            pkCmd.BindByName = true;
            pkCmd.CommandText = pkSql;
            AddOwnerParam(pkCmd);
            pkCmd.Parameters.Add(new OracleParameter("p_table", tableName));
            using var pkReader = await pkCmd.ExecuteReaderAsync(ct);
            while (await pkReader.ReadAsync(ct))
            {
                pkColumns.Add(pkReader.GetString(0));
            }
        }

        // Get unique constraint columns
        string uniqueSql;
        if (hasOwner)
        {
            uniqueSql = $@"
            SELECT cols.column_name
            FROM {consView} cons
            JOIN {consColView} cols ON cons.constraint_name = cols.constraint_name 
                AND cons.owner = cols.owner
            WHERE cons.constraint_type = 'U' 
              AND cons.owner = :p_owner 
              AND cons.table_name = :p_table";
        }
        else
        {
            uniqueSql = $@"
            SELECT cols.column_name
            FROM {consView} cons
            JOIN {consColView} cols ON cons.constraint_name = cols.constraint_name 
            WHERE cons.constraint_type = 'U' 
              AND cons.table_name = :p_table";
        }
        
        var uniqueColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var uniqueCmd = connection.CreateCommand())
        {
            uniqueCmd.BindByName = true;
            uniqueCmd.CommandText = uniqueSql;
            AddOwnerParam(uniqueCmd);
            uniqueCmd.Parameters.Add(new OracleParameter("p_table", tableName));
            using var uniqueReader = await uniqueCmd.ExecuteReaderAsync(ct);
            while (await uniqueReader.ReadAsync(ct))
            {
                uniqueColumns.Add(uniqueReader.GetString(0));
            }
        }

        // Get table size
        long? sizeBytes = null;
        try
        {
            var segmentsView = GetView("SEGMENTS");
            var sizeSql = hasOwner
                ? $"SELECT bytes FROM {segmentsView} WHERE owner = :p_owner AND segment_name = :p_table AND segment_type = 'TABLE'"
                : $"SELECT bytes FROM {segmentsView} WHERE segment_name = :p_table AND segment_type = 'TABLE'";
                
            using var sizeCmd = connection.CreateCommand();
            sizeCmd.BindByName = true;
            sizeCmd.CommandText = sizeSql;
            AddOwnerParam(sizeCmd);
            sizeCmd.Parameters.Add(new OracleParameter("p_table", tableName));
            var sizeResult = await sizeCmd.ExecuteScalarAsync(ct);
            sizeBytes = sizeResult == null || sizeResult == DBNull.Value ? null : Convert.ToInt64(sizeResult);
        }
        catch { /* Size info not available */ }

        // Build column list
        var columns = new List<TargetColumnInfo>();
        _logger.LogDebug("Retrieving columns with SQL: {Sql}", columnsSql);
        using var reader = await columnsCmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var colName = reader.GetString(0);
            var dataType = reader.GetString(1);
            var isNullable = reader.GetString(2) == "Y";
            var dataLength = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3);
            var precision = reader.IsDBNull(4) ? (int?)null : reader.GetInt32(4);
            var scale = reader.IsDBNull(5) ? (int?)null : reader.GetInt32(5);
            var charLength = reader.IsDBNull(6) ? (int?)null : reader.GetInt32(6);

            var nativeType = BuildOracleNativeType(dataType, dataLength, precision, scale, charLength);
            var maxLength = charLength ?? dataLength;

            columns.Add(new TargetColumnInfo(
                colName,
                nativeType,
                MapOracleToClr(dataType),
                isNullable,
                pkColumns.Contains(colName),
                uniqueColumns.Contains(colName),
                maxLength,
                precision,
                scale
            ));
        }

        return new TargetSchemaInfo(
            columns,
            true,
            rowCount,
            sizeBytes,
            pkColumns.Count > 0 ? pkColumns.ToList() : null
        );
    }

    private static (string owner, string table) ParseTableName(string fullName)
    {
        if (string.IsNullOrEmpty(fullName)) return ("", "");

        // Simple parser that handles "Schema"."Table" or Schema.Table
        // We assume logic coming from ResolveTargetTableAsync puts quotes correctly if needed.
        
        string[] parts;
        if (fullName.Contains("\".\"")) // "Schema"."Table"
        {
             parts = fullName.Split(new[] { "\".\"" }, StringSplitOptions.None);
             if (parts.Length == 2) 
             {
                 return (parts[0].Trim('"'), parts[1].Trim('"'));
             }
        }
        else if (fullName.Contains("."))
        {
            parts = fullName.Split('.');
            if (parts.Length == 2)
            {
                var p0 = parts[0].Trim('"');
                var p1 = parts[1].Trim('"');
                // If not quoted, uppercase it? No, ResolveTargetTableAsync already handles casing/quoting logic.
                // But if it came from _options.Table (Recreate strategy), we might need normalization?
                // Let's rely on IsQuoted verification.
                
                return (NormalizeIdentifier(parts[0]), NormalizeIdentifier(parts[1]));
            }
        }

        return ("", NormalizeIdentifier(fullName));
    }

    private static string NormalizeIdentifier(string identifier)
    {
        if (identifier.StartsWith("\"") && identifier.EndsWith("\""))
        {
            return identifier.Trim('"');
        }
        // Unquoted -> Upper
        return identifier.ToUpperInvariant();
    }

    private static string BuildOracleNativeType(string dataType, int? dataLength, int? precision, int? scale, int? charLength)
    {
        return dataType.ToUpperInvariant() switch
        {
            "VARCHAR2" when charLength.HasValue => $"VARCHAR2({charLength})",
            "CHAR" when charLength.HasValue => $"CHAR({charLength})",
            "NVARCHAR2" when charLength.HasValue => $"NVARCHAR2({charLength})",
            "NCHAR" when charLength.HasValue => $"NCHAR({charLength})",
            "NUMBER" when precision.HasValue && scale.HasValue && scale > 0 => $"NUMBER({precision},{scale})",
            "NUMBER" when precision.HasValue => $"NUMBER({precision})",
            "RAW" when dataLength.HasValue => $"RAW({dataLength})",
            _ => dataType.ToUpperInvariant()
        };
    }

    private static Type? MapOracleToClr(string dataType)
    {
        return dataType.ToUpperInvariant() switch
        {
            "NUMBER" => typeof(decimal),
            "INTEGER" => typeof(int),
            "FLOAT" => typeof(double),
            "BINARY_FLOAT" => typeof(float),
            "BINARY_DOUBLE" => typeof(double),
            "VARCHAR2" or "NVARCHAR2" or "CHAR" or "NCHAR" or "CLOB" or "NCLOB" => typeof(string),
            "DATE" or "TIMESTAMP" => typeof(DateTime),
            "TIMESTAMP WITH TIME ZONE" or "TIMESTAMP WITH LOCAL TIME ZONE" => typeof(DateTimeOffset),
            "RAW" or "BLOB" => typeof(byte[]),
            _ => typeof(string)
        };
    }

    #endregion

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        _targetTableName = _options.Table;
        
        // GLOBAL NORMALIZATION:
        // Create a secure list of columns where names are normalized if not case-sensitive.
        // This ensures consistence across Create Table, Insert, BulkCopy, etc.
        var normalizedColumns = new List<ColumnInfo>(columns.Count);
        foreach (var col in columns)
        {
            if (col.IsCaseSensitive)
            {
                normalizedColumns.Add(col);
            }
            else
            {
                // Unquoted/Insensitive -> Normalize to Oracle default (UPPERCASE)
                // e.g. "id" -> "ID"
                normalizedColumns.Add(col with { Name = _dialect.Normalize(col.Name) });
            }
        }
        _columns = normalizedColumns;
        
        _logger.LogInformation("Initializing Oracle Writer for table {Table} (WriteStrategy={Strategy})", _targetTableName, _options.Strategy);
        await _connection.OpenAsync(ct);


        if (_options.Strategy == OracleWriteStrategy.Recreate)
        {
            // STRATEGY: RECREATE
            // 1. Drop existing table
            try {
                using var dropCmd = _connection.CreateCommand();
                var sql = $"DROP TABLE {_targetTableName}";
                dropCmd.CommandText = sql;
                await dropCmd.ExecuteNonQueryAsync(ct);
                _logger.LogInformation("Dropped table {Table} (Recreate Strategy)", _targetTableName);
            } 
            catch (OracleException ex) when (ex.Number == 942) { /* ORA-00942: table or view does not exist */ }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Recreate Drop failed. SQL: DROP TABLE {_targetTableName}. Error: {ex.Message}", ex);
            }

            // 2. Create new table
            var createTableSql = BuildCreateTableSql(_targetTableName, normalizedColumns);
            try
            {
                using var cmd = _connection.CreateCommand();
                cmd.CommandText = createTableSql;
                await cmd.ExecuteNonQueryAsync(ct);
                _logger.LogInformation("Created table {Table}", _targetTableName);
            }
            catch (Exception ex)
            {
                 throw new InvalidOperationException($"Create Table failed. SQL: {createTableSql}{Environment.NewLine}Error: {ex.Message}", ex);
            }
        }
        else
        {
            // STRATEGY: INSERT, UPSERT, IGNORE, DELETE, TRUNCATE, APPEND
            // The table MUST exist. We do NOT create it automatically to avoid implicit schema definition issues.
            try
            {
                // Native Resolution: Ask Oracle "What table is this?" (Handles Synonyms, Casing, Schema Resolution)
                var resolved = await ResolveTargetTableAsync(_options.Table, ct);
                
                // Update target table name with the REAL object name (Schema.Table)
                // Use "Smart Quoting" to respect user's preference (minimize quotes)
                var safeSchema = GetSmartQuotedIdentifier(resolved.Schema);
                var safeTable = GetSmartQuotedIdentifier(resolved.Table);
                
                // Store globally for subsequent queries (Introspection, DML)
                _targetTableName = $"{safeSchema}.{safeTable}";
                
                _logger.LogInformation("Resolved table {Input} to {Resolved} (Schema: {Schema})", _options.Table, _targetTableName, resolved.Schema);
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

            // Introspect target schema to get actual column types (Fix for ORA-00932 BLOB/RAW mismatch)
            try 
            {
                var schemaInfo = await InspectTargetAsync(ct);
                if (schemaInfo != null)
                {
                    _targetColumnTypes = new Dictionary<string, OracleDbType>(StringComparer.OrdinalIgnoreCase);
                    foreach(var col in schemaInfo.Columns)
                    {
                        var dbType = MapNativeTypeToOracleDbType(col.NativeType);
                        if (dbType.HasValue)
                        {
                            _targetColumnTypes[col.Name] = dbType.Value;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
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
            if (targetInfo?.PrimaryKeyColumns != null)
            {
                _keyColumns.AddRange(targetInfo.PrimaryKeyColumns);
            }

            if (_keyColumns.Count == 0 && !string.IsNullOrEmpty(_options.Key))
            {
                 _keyColumns.AddRange(_options.Key.Split(',').Select(k => k.Trim()));
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
                 isUpsert);
             
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
            _bulkCopy.DestinationTableName = _targetTableName;
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
                useAppendHint);
            
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
             
             _logger.LogDebug("Executing Merge for batch of {Count} rows", rows.Count);
             await BindAndExecuteAsync(_mergeCommand, _mergeParameters, rows, ct);
             return;
        }

        if (_options.InsertMode == OracleInsertMode.Standard || _options.InsertMode == OracleInsertMode.Append)
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
                        colValues[rowIndex] = val;
                    }
                }
                _insertParameters[colIndex].Value = colValues;
            }

            try
            {
                await _insertCommand.ExecuteNonQueryAsync(ct);
                _logger.LogDebug("Inserted {Count} rows via Array Binding ({Mode})...", rowCount, _options.InsertMode);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Array Binding Insert Failed.{Environment.NewLine}SQL: {_insertCommand.CommandText}{Environment.NewLine}Error: {ex.Message}", ex);
            }
        }
        else
        {
            if (_bulkCopy == null) throw new InvalidOperationException("BulkCopy not initialized");
            
            _bulkCopy.BatchSize = rows.Count;
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
                _logger.LogWarning(ex, "OracleBulkCopy failed. Starting in-depth analysis of the batch...");
                var analysis = await BatchFailureAnalyzer.AnalyzeAsync(this, rows, _columns, ct);
                if (!string.IsNullOrEmpty(analysis))
                {
                    throw new InvalidOperationException($"Bulk Copy Failed with detailed analysis:\n{analysis}", ex);
                }
                throw; // Rethrow original if we couldn't find the specific culprit
            }
            watch.Stop();
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
                        colValues[rowIndex] = val;
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



    private string BuildCreateTableSql(string tableName, IReadOnlyList<ColumnInfo> columns)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE {tableName} (");
        
        for (int i = 0; i < columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            // Columns are passed from _columns which is already globally normalized
            sb.Append($"{SqlIdentifierHelper.GetSafeIdentifier(_dialect, columns[i])} {OracleTypeMapper.MapToProviderType(columns[i].ClrType)}");
        }

        if (!string.IsNullOrEmpty(_options.Key))
        {
             var keyNames = _options.Key.Split(',').Select(k => k.Trim());
             var safeKeys = new List<string>();
             foreach(var k in keyNames)
             {
                 var col = columns.FirstOrDefault(c => string.Equals(c.Name, k, StringComparison.OrdinalIgnoreCase));
                 if (col != null)
                 {
                     safeKeys.Add(SqlIdentifierHelper.GetSafeIdentifier(_dialect, col));
                 }
                 else
                 {
                     safeKeys.Add(_dialect.Quote(k));
                 }
             }
             sb.Append($", PRIMARY KEY ({string.Join(", ", safeKeys)})");
        }
        
        sb.Append(")");
        return sb.ToString();
    }

    private static OracleDbType? MapNativeTypeToOracleDbType(string nativeType)
    {
        // nativeType often includes size: "RAW(16)", "VARCHAR2(100)", "NUMBER(10,2)"
        // We only care about the base type name
        var parenIndex = nativeType.IndexOf('(');
        var baseType = parenIndex > 0 ? nativeType.Substring(0, parenIndex) : nativeType;
        
        return baseType.ToUpperInvariant() switch
        {
            "RAW" => OracleDbType.Raw,
            "BLOB" => OracleDbType.Blob,
            "CLOB" => OracleDbType.Clob,
            "NCLOB" => OracleDbType.NClob,
            "DATE" => OracleDbType.Date,
            "TIMESTAMP" => OracleDbType.TimeStamp,
            "TIMESTAMP WITH TIME ZONE" => OracleDbType.TimeStampTZ,
            "TIMESTAMP WITH LOCAL TIME ZONE" => OracleDbType.TimeStampLTZ,
            "VARCHAR2" => OracleDbType.Varchar2,
            "NVARCHAR2" => OracleDbType.NVarchar2,
            "CHAR" => OracleDbType.Char,
            "NCHAR" => OracleDbType.NChar,
            "NUMBER" => OracleDbType.Decimal,
            "FLOAT" => OracleDbType.BinaryDouble, // Or Single depending on precision
            "BINARY_FLOAT" => OracleDbType.BinaryFloat,
            "BINARY_DOUBLE" => OracleDbType.BinaryDouble,
            "INTERVAL YEAR TO MONTH" => OracleDbType.IntervalYM,
            "INTERVAL DAY TO SECOND" => OracleDbType.IntervalDS,
            _ => null // Keep default mapping
        };
    }

    private async Task<(string Schema, string Table)> ResolveTargetTableAsync(string inputName, CancellationToken ct)
    {
        // DBMS_UTILITY.NAME_RESOLVE signature:
        // PROCEDURE NAME_RESOLVE (
        //    name          IN  VARCHAR2, 
        //    context       IN  NUMBER,
        //    schema        OUT VARCHAR2, 
        //    part1         OUT VARCHAR2, 
        //    part2         OUT VARCHAR2,
        //    dblink        OUT VARCHAR2, 
        //    part1_type    OUT NUMBER, 
        //    object_number OUT NUMBER);

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
            DECLARE
              v_schema VARCHAR2(30);
              v_part1  VARCHAR2(30);
              v_part2  VARCHAR2(30);
              v_dblink VARCHAR2(30);
              v_part1_type NUMBER;
              v_object_number NUMBER;
            BEGIN
              DBMS_UTILITY.NAME_RESOLVE(
                name => :name, 
                context => 2, 
                schema => v_schema, 
                part1 => v_part1, 
                part2 => v_part2, 
                dblink => v_dblink, 
                part1_type => v_part1_type, 
                object_number => v_object_number
              );
              :out_schema := v_schema;
              :out_table := v_part1; 
            END;";
            
        cmd.Parameters.Add(new OracleParameter("name", inputName));
        
        var pSchema = new OracleParameter("out_schema", OracleDbType.Varchar2, 100) { Direction = ParameterDirection.Output };
        var pTable = new OracleParameter("out_table", OracleDbType.Varchar2, 100) { Direction = ParameterDirection.Output };
        
        cmd.Parameters.Add(pSchema);
        cmd.Parameters.Add(pTable);

        await cmd.ExecuteNonQueryAsync(ct);

        string schema = pSchema.Value.ToString() ?? "";
        string table = pTable.Value.ToString() ?? "";
        
        return (schema, table);
    }
    
    private static string GetSmartQuotedIdentifier(string identifier)
    {
        if (string.IsNullOrEmpty(identifier)) return "";
        
        // Oracle standard identifier definition:
        // - Starts with letter
        // - Contains only letters, numbers, _, $, #
        // - Is not a reserved word (simplified check here, we mainly care about chars)
        
        // If it contains any non-standard char, quote it.
        // Also if it was mixed case in DB, it will come back mixed case from NAME_RESOLVE.
        // But NAME_RESOLVE returns the stored casing. 
        // If stored as "MyTable", input was "MyTable". Output is "MyTable". We MUST quote it.
        // If stored as "MYTABLE", input was "mytable". Output is "MYTABLE". We can leave unquoted.
        
        // Simple heuristic: If it's all UPPERCASE and valid chars, don't quote.
        // Otherwise, quote.
        
        bool isAllUpper = true;
        bool isValidChars = true;
        
        if (!char.IsLetter(identifier[0])) isValidChars = false;
        
        foreach(var c in identifier)
        {
            if (char.IsLower(c)) isAllUpper = false;
            if (!char.IsLetterOrDigit(c) && c != '_' && c != '$' && c != '#') isValidChars = false;
        }
        
        if (isAllUpper && isValidChars) return identifier;
        
        return $"\"{identifier}\"";
    }
}
