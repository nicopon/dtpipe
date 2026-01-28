using Oracle.ManagedDataAccess.Client;
using QueryDump.Core.Abstractions;
using QueryDump.Cli.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Core.Options;
using System.Data;
using System.Text;

using QueryDump.Adapters.Oracle;
using Microsoft.Extensions.Logging;
using QueryDump.Core.Helpers;

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


    public long BytesWritten => 0;

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
        _logger.LogDebug("Starting target schema inspection for table {Table}", _options.Table);
        await using var connection = new OracleConnection(_connectionString);
        await connection.OpenAsync(ct);

        var (owner, tableName) = ParseTableName(_options.Table);
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
        // Remove quotes and parse owner.table
        var cleaned = fullName.Replace("\"", "");
        var parts = cleaned.Split('.');
        if (parts.Length == 2)
        {
            return (parts[0].ToUpperInvariant(), parts[1].ToUpperInvariant());
        }
        // Determine owner and table name
        return ("", parts[0].ToUpperInvariant());
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
        var targetTable = NormalizeTableName(_options.Table);
        Console.WriteLine($"[OracleDataWriter] Target Table (Raw): '{_options.Table}' -> Normalized: '{targetTable}'");
        
        _targetTableName = targetTable;
        _columns = columns;
        
        _logger.LogInformation("Initializing Oracle Writer for table {Table} (WriteStrategy={Strategy})", _targetTableName, _options.Strategy);
        await _connection.OpenAsync(ct);


        if (_options.Strategy == OracleWriteStrategy.DeleteThenInsert)
        {
            try {
                using var deleteCmd = _connection.CreateCommand();
                deleteCmd.CommandText = $"DELETE FROM {_targetTableName}";
                await deleteCmd.ExecuteNonQueryAsync(ct);
                _logger.LogInformation("Deleted existing rows from table {Table}", _targetTableName);
            } catch (OracleException ex) when (ex.Number == 942) { /* ORA-00942: table or view does not exist */ }
        }
        else if (_options.Strategy == OracleWriteStrategy.Truncate)
        {
             try {
                using var truncCmd = _connection.CreateCommand();
                truncCmd.CommandText = $"TRUNCATE TABLE {_targetTableName}";
                await truncCmd.ExecuteNonQueryAsync(ct);
                _logger.LogInformation("Truncated table {Table}", _targetTableName);
            } catch (OracleException ex) when (ex.Number == 942) { /* Table doesn't exist, ignore */ }
        }

        var createTableSql = BuildCreateTableSql(_targetTableName, columns);
        
        try
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = createTableSql;
            await cmd.ExecuteNonQueryAsync(ct);
            _logger.LogInformation("Created table {Table} (or verified existence)", _targetTableName);
        }
        catch (OracleException ex) when (ex.Number == 955) // ORA-00955: name already used
        {
            // Expected if table exists
        }

        // Initialize reusable objects
        InitializeCommands();
    }

    private void InitializeCommands()
    {
        if (_options.InsertMode == OracleInsertMode.Bulk)
        {
            _bulkCopy = new OracleBulkCopy(_connection);
            _bulkCopy.DestinationTableName = _targetTableName;
            _bulkCopy.BulkCopyTimeout = 0;
            foreach (var col in _columns!)
            {
                _bulkCopy.ColumnMappings.Add(col.Name, col.Name);
            }
        }
        else
        {
            // Build parameterized INSERT statement once
            var sb = new StringBuilder();
            sb.Append("INSERT ");
            
            if (_options.InsertMode == OracleInsertMode.Append)
            {
                sb.Append("/*+ APPEND */ ");
            }

            sb.Append($"INTO {_targetTableName} (");
            for (int i = 0; i < _columns!.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                sb.Append($"\"{_columns[i].Name}\"");
            }
            sb.Append(") VALUES (");
            for (int i = 0; i < _columns.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                sb.Append($":v{i}");
            }
            sb.Append(")");
            
            var insertSql = sb.ToString();
            _logger.LogDebug("Generated Insert SQL: {Sql}", insertSql);

            _insertCommand = _connection.CreateCommand();
            _insertCommand.BindByName = true;
            _insertCommand.CommandText = insertSql;
            
            _insertParameters = new OracleParameter[_columns.Count];
            for (int i = 0; i < _columns.Count; i++)
            {
                _insertParameters[i] = _insertCommand.CreateParameter();
                _insertParameters[i].ParameterName = $"v{i}";               
                _insertParameters[i].OracleDbType = OracleTypeMapper.GetOracleDbType(_columns[i].ClrType);
                
                _insertCommand.Parameters.Add(_insertParameters[i]);
            }
        }
    }



    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_columns is null) throw new InvalidOperationException("Not initialized");



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
                // Fallback analysis could be added here if needed
                throw new InvalidOperationException($"Array Binding Insert Failed. Error: {ex.Message}", ex);
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

        await _connection.DisposeAsync();
    }

    private string BuildCreateTableSql(string tableName, IReadOnlyList<ColumnInfo> columns)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE {tableName} (");
        
        for (int i = 0; i < columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"\"{columns[i].Name}\" {OracleTypeMapper.MapToProviderType(columns[i].ClrType)}");
        }
        
        sb.Append(")");
        return sb.ToString();
    }




    private static string NormalizeTableName(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName)) return tableName;

        var parts = tableName.Split('.');
        for (int i = 0; i < parts.Length; i++)
        {
            var part = parts[i];
            
            // If already quoted, leave it as is
            if (part.StartsWith("\"") && part.EndsWith("\""))
            {
                continue;
            }

            if (IsSimpleIdentifier(part))
            {
                parts[i] = part.ToUpperInvariant();
            }
            else
            {
                parts[i] = $"\"{part.ToUpperInvariant()}\"";
            }
        }
        
        return string.Join(".", parts);
    }

    private static bool IsSimpleIdentifier(string name)
    {
        if (string.IsNullOrEmpty(name)) return false;
        
        // Oracle simple identifiers:
        // 1. Must define max length (30 or 128 depending on version, but we just check chars)
        // 2. Start with a letter
        // 3. Contain only alphanumeric, _, $, #
        
        if (!char.IsLetter(name[0])) return false;

        for (int i = 1; i < name.Length; i++)
        {
            char c = name[i];
            if (!char.IsLetterOrDigit(c) && c != '_' && c != '$' && c != '#')
            {
                return false;
            }
        }
        return true;
    }


}
