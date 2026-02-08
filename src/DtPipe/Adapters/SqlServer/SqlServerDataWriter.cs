using Microsoft.Data.SqlClient;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using System.Data;
using DtPipe.Core.Helpers;
using System.Text;

namespace DtPipe.Adapters.SqlServer;

public class SqlServerDataWriter : IDataWriter, ISchemaInspector, IKeyValidator
{
    private readonly string _connectionString;
    private readonly SqlServerWriterOptions _options;
    private SqlConnection? _connection;
    private SqlBulkCopy? _bulkCopy;
    private DataTable? _bufferTable;

    private IReadOnlyList<PipeColumnInfo>? _columns;
    private string _targetTableName = ""; // Resolved or Fallback
    private List<string> _keyColumns = new();
    private bool _isDbCaseSensitive = false;
    
    private static async Task<(string Schema, string Table)?> ResolveTableAsync(SqlConnection connection, string inputName, CancellationToken ct)
    {
        var sql = @"
            SELECT 
                OBJECT_SCHEMA_NAME(OBJECT_ID(@input)), 
                OBJECT_NAME(OBJECT_ID(@input))
            WHERE OBJECT_ID(@input) IS NOT NULL";

        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@input", inputName);

        using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
             if (!reader.IsDBNull(0) && !reader.IsDBNull(1))
             {
                 return (reader.GetString(0), reader.GetString(1));
             }
        }
        return null;
    }

    private static (string Schema, string Table) ParseTableName(string tableName)
    {
        // Simple fallback parser
        if (string.IsNullOrWhiteSpace(tableName)) return ("dbo", tableName); // dbo is default
        var parts = tableName.Split('.');
        if (parts.Length == 2) return (parts[0].Trim('[',']'), parts[1].Trim('[',']'));
        return ("dbo", tableName.Trim('[',']'));
    }

    private string GetSmartQuotedIdentifier(string identifier)
    {
        // Quote only if necessary (special chars, reserved words)
        if (string.IsNullOrEmpty(identifier)) return identifier;
        
        bool needsQuoting = _dialect.NeedsQuoting(identifier);
        
        // If DB is case sensitive, any mixed-case or lower-case identifier should probably be quoted 
        // to be safe, although SQL Server's rules for unquoted identifiers in CS collation 
        // are that they must match exactly.
        if (!needsQuoting && _isDbCaseSensitive)
        {
            // If the identifier has ANY lowercase letters and we are in a CS collation, 
            // it's safer to quote it if we want to preserve exactly that case.
            if (identifier.Any(char.IsLower))
            {
                needsQuoting = true;
            }
        }

        return needsQuoting ? _dialect.Quote(identifier) : identifier;
    }

    private async Task DetectDatabaseCollationAsync(CancellationToken ct)
    {
        if (_connection == null) return;
        
        try
        {
            var dbCmd = new SqlCommand("SELECT collation_name FROM sys.databases WHERE name = DB_NAME()", _connection);
            var result = await dbCmd.ExecuteScalarAsync(ct);
            if (result != null && result != DBNull.Value)
            {
                var collation = (string)result;
                _isDbCaseSensitive = collation.Contains("_CS", StringComparison.OrdinalIgnoreCase);
            }
        }
        catch
        {
            // Fallback to safe default
            _isDbCaseSensitive = false;
        }
    }
    private List<int> _keyIndices = new();

    private readonly ISqlDialect _dialect = new DtPipe.Core.Dialects.SqlServerDialect();
    public ISqlDialect Dialect => _dialect;

    public SqlServerDataWriter(string connectionString, SqlServerWriterOptions options)
    {
        _connectionString = connectionString;
        _options = options;
    }

    public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        // Column and table normalization logic
        // Create a secure list of columns where names are normalized if not case-sensitive.
        // This ensures consistency across CREATE TABLE, INSERT, BULK COPY, MERGE, etc.
        var normalizedColumns = new List<PipeColumnInfo>(columns.Count);
        foreach (var col in columns)
        {
            if (col.IsCaseSensitive)
            {
                normalizedColumns.Add(col);
            }
            else
            {
                // Unquoted/Insensitive -> Normalize to SQL Server default (lowercase for consistency)
                // Note: SQL Server is case-insensitive by default, but we normalize for consistency
                // e.g. "UserName" -> "username"
                normalizedColumns.Add(col with { Name = _dialect.Normalize(col.Name) });
            }
        }

        _columns = normalizedColumns;
        
        if (_connection == null)
        {
            _connection = new SqlConnection(_connectionString);
        }
        if (_connection.State != ConnectionState.Open)
        {
            await _connection.OpenAsync(ct);
        }

        // Detect DB Case Sensitivity early
        await DetectDatabaseCollationAsync(ct);

        // Initialize Buffer Table and Bulk Copy early so sync logic can update them
        _bufferTable = new DataTable();
        _bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null)
        {
            DestinationTableName = _options.Table, // Will be updated later
            BatchSize = 0, 
            BulkCopyTimeout = 0
        };

        // Table name resolution logic
        string resolvedSchema;
        string resolvedTable;
        
        var resolved = await ResolveTableAsync(_connection, _options.Table, ct);
        if (resolved != null)
        {
            resolvedSchema = resolved.Value.Schema;
            resolvedTable = resolved.Value.Table;
        }
        else
        {
            if (_options.Strategy == SqlServerWriteStrategy.Recreate)
            {
                var (s, t) = ParseTableName(_options.Table);
                resolvedSchema = s;
                resolvedTable = t;
            }
            else
            {
                throw new InvalidOperationException($"Table '{_options.Table}' does not exist or could not be resolved (Synonym/Path). Strategy {_options.Strategy} requires an existing table.");
            }
        }
        
        // Construct canonical Quoted Name for SQL usage
        // Use smart quoting: only quote if necessary (mixed case, special chars, reserved words)
        // Note: ResolveTableAsync returns unquoted names from DB metadata.
        var safeSchema = GetSmartQuotedIdentifier(resolvedSchema);
        var safeTable = GetSmartQuotedIdentifier(resolvedTable);
        _targetTableName = $"{safeSchema}.{safeTable}";
        
        if (_options.Strategy == SqlServerWriteStrategy.Recreate)
        {
            TargetSchemaInfo? existingSchema = null;
            if (resolved != null)
            {
                // Table exists. Introspect BEFORE Drop to preserve native types.
                try 
                {
                    existingSchema = await InspectTargetAsync(ct);
                }
                catch { /* Ignore introspection errors during recreate, treat as not existing */ }
                
                // DROP uses quoted name
                var cmd = new SqlCommand($"DROP TABLE IF EXISTS {_targetTableName}", _connection);
                await cmd.ExecuteNonQueryAsync(ct);
            }

            // Create using canonical names
            string createSql;
            if (existingSchema != null && existingSchema.Exists)
            {
                 createSql = BuildCreateTableFromIntrospection(_targetTableName, existingSchema);
            }
            else
            {
                 createSql = BuildCreateTableSql(resolvedSchema, resolvedTable, columns);
            }

            var createCmd = new SqlCommand(createSql, _connection);
            await createCmd.ExecuteNonQueryAsync(ct);

            // Sync columns metadata from introspection to ensure future DML (BulkCopy) matches exact case/quotes
            if (existingSchema != null)
            {
                var newCols = new List<PipeColumnInfo>(_columns!.Count);
                foreach (var col in _columns!)
                {
                    var introspected = existingSchema.Columns.FirstOrDefault(c => c.Name.Equals(col.Name, StringComparison.OrdinalIgnoreCase));
                    if (introspected != null)
                    {
                        newCols.Add(col with { Name = introspected.Name, IsCaseSensitive = introspected.IsCaseSensitive });
                    }
                    else
                    {
                        newCols.Add(col);
                    }
                }
                _columns = newCols;

                // Re-initialize buffer table columns if names changed
                _bufferTable!.Columns.Clear();
                _bulkCopy!.ColumnMappings.Clear();
                foreach (var col in _columns)
                {
                    var type = Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType;
                    _bufferTable.Columns.Add(col.Name, type);
                    _bulkCopy.ColumnMappings.Add(col.Name, col.Name);
                }
            }
        }
        else if (_options.Strategy == SqlServerWriteStrategy.Truncate)
        {
            var cmd = new SqlCommand($"TRUNCATE TABLE {_targetTableName}", _connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        else if (_options.Strategy == SqlServerWriteStrategy.DeleteThenInsert)
        {
            var cmd = new SqlCommand($"DELETE FROM {_targetTableName}", _connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        else
        {
            // For Append/Upsert/Ignore/etc., if table resolved -> Good.
            // If NOT resolved -> We must try to create it.
            if (resolved == null)
            {
                // Native resolution failed, so it likely doesn't exist.
                // Fallback to manual parsing for creation.
                var (s, t) = ParseTableName(_options.Table);
                resolvedSchema = s;
                resolvedTable = t;
                _targetTableName = $"[{resolvedSchema}].[{resolvedTable}]";

                var createSql = BuildCreateTableSql(resolvedSchema, resolvedTable, columns);
                var createCmd = new SqlCommand(createSql, _connection);
                try 
                {
                    await createCmd.ExecuteNonQueryAsync(ct);
                }
                catch (SqlException ex) when (ex.Number == 2714) // Table already exists (race condition)
                {
                   // Ignore
                }
            }
            // If resolved != null, the table exists, and _targetTableName is set.
        }
        
        // Update BulkCopy destination with the resolved quoted name
        _bulkCopy!.DestinationTableName = _targetTableName;

        // Initialize Column Mappings and Buffer Table Schema
        _bufferTable!.Columns.Clear();
        _bulkCopy.ColumnMappings.Clear();
        foreach (var col in _columns)
        {
            var type = Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType;
            _bufferTable.Columns.Add(col.Name, type); 
            _bulkCopy.ColumnMappings.Add(col.Name, col.Name);
        }

        if (_options.Strategy == SqlServerWriteStrategy.Upsert || _options.Strategy == SqlServerWriteStrategy.Ignore)
        {
             // 1. Resolve Keys
            var targetInfo = await InspectTargetAsync(ct);
            if (targetInfo?.PrimaryKeyColumns != null)
            {
                _keyColumns.AddRange(targetInfo.PrimaryKeyColumns);
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
    }

    #region ISchemaInspector Implementation

    public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        // Resolve target using native logic
        var resolved = await ResolveTableAsync(connection, _options.Table, ct);
        if (resolved == null)
        {
             return new TargetSchemaInfo([], false, null, null, null);
        }
        
        var (schema, table) = resolved.Value;

        // 1. Ensure we have the latest DB collation info
        await DetectDatabaseCollationAsync(ct);

        // 2. Get Columns
        var cols = new List<TargetColumnInfo>();
        {
            var colCmd = new SqlCommand(@"
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLLATION_NAME, DATETIME_PRECISION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = @Table
                ORDER BY ORDINAL_POSITION", connection);
            colCmd.Parameters.AddWithValue("@Schema", schema);
            colCmd.Parameters.AddWithValue("@Table", table);

            using var reader = await colCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var name = reader.GetString(0);
                var type = reader.GetString(1);
                var nullable = reader.GetString(2) == "YES";
                var maxLength = reader.IsDBNull(3) ? null : (int?)Convert.ToInt32(reader[3]);
                var precision = reader.IsDBNull(4) ? null : (int?)Convert.ToInt32(reader[4]);
                var scale = reader.IsDBNull(5) ? null : (int?)Convert.ToInt32(reader[5]);
                var collation = reader.IsDBNull(6) ? null : reader.GetString(6);
                var datetimePrecision = reader.IsDBNull(7) ? null : (int?)Convert.ToInt32(reader[7]);

                // NUMERIC_PRECISION is null for datetime types, so we use datetimePrecision if available
                var finalPrecision = precision ?? datetimePrecision;

                // Collation based Case Sensitivity (preferred over simple name casing for SQL Server)
                // If it's a non-text type (numeric/etc), it inherits behavior or is irrelevant.
                // If text type, it clearly indicates CS/CI.
                bool isColCaseSensitive = collation != null 
                    ? collation.Contains("_CS", StringComparison.OrdinalIgnoreCase)
                    : _isDbCaseSensitive; // Inherit from DB if per-column collation is null

                cols.Add(new TargetColumnInfo(
                    name,
                    type,
                    SqlServerTypeMapper.MapFromProviderType(type),
                    nullable,
                    false,
                    false,
                    maxLength,
                    finalPrecision,
                    scale,
                    IsCaseSensitive: isColCaseSensitive
                ));
            }
        }

        // Get Primary Key
        var pkCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        {
            var pkCmd = new SqlCommand(@"
                SELECT KCU.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU ON TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
                    AND TC.TABLE_SCHEMA = KCU.TABLE_SCHEMA
                WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                  AND TC.TABLE_SCHEMA = @Schema 
                  AND TC.TABLE_NAME = @Table
                ORDER BY KCU.ORDINAL_POSITION", connection);
            pkCmd.Parameters.AddWithValue("@Schema", schema);
            pkCmd.Parameters.AddWithValue("@Table", table);
            
            using var pkReader = await pkCmd.ExecuteReaderAsync(ct);
            while(await pkReader.ReadAsync(ct))
            {
                pkCols.Add(pkReader.GetString(0));
            }
        }

        // Get Unique Columns
        var uniqueCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        {
            var uqCmd = new SqlCommand(@"
                SELECT KCU.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU ON TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
                    AND TC.TABLE_SCHEMA = KCU.TABLE_SCHEMA
                WHERE TC.CONSTRAINT_TYPE = 'UNIQUE' 
                  AND TC.TABLE_SCHEMA = @Schema 
                  AND TC.TABLE_NAME = @Table
                ORDER BY KCU.ORDINAL_POSITION", connection);
            uqCmd.Parameters.AddWithValue("@Schema", schema);
            uqCmd.Parameters.AddWithValue("@Table", table);
            
            using var uqReader = await uqCmd.ExecuteReaderAsync(ct);
            while(await uqReader.ReadAsync(ct))
            {
                uniqueCols.Add(uqReader.GetString(0));
            }
        }

        // Get Row Count Estimate (sys.partitions)
        var countCmd = new SqlCommand(@"
            SELECT SUM(p.rows) 
            FROM sys.partitions p
            JOIN sys.tables t ON p.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = @Table AND s.name = @Schema AND p.index_id < 2", connection);
        countCmd.Parameters.AddWithValue("@Schema", schema);
        countCmd.Parameters.AddWithValue("@Table", table);
        
        var countResult = await countCmd.ExecuteScalarAsync(ct);
        long? rowCount = countResult != null && countResult != DBNull.Value ? Convert.ToInt64(countResult) : null;

        return new TargetSchemaInfo(
            cols, 
            true, 
            rowCount, 
            null, 
            pkCols.Count > 0 ? pkCols.ToList() : null,
            uniqueCols.Count > 0 ? uniqueCols.ToList() : null,
            IsRowCountEstimate: true // SQL Server sys.partitions is an estimate
        );
    }

    #endregion
    

    /// <summary>
    /// Builds CREATE TABLE DDL from source column info.
    /// </summary>
    /// <remarks>
    /// NOTE: Types are mapped from CLR types (e.g., decimal → DECIMAL, string → NVARCHAR),
    /// not preserved from target schema. Type precision, scale, and length constraints
    /// may differ from the original table when using the Recreate strategy.
    /// 
    /// For exact structure preservation, use Append strategy or manage DDL separately.
    /// </remarks>
    private string BuildCreateTableSql(string schema, string table, IReadOnlyList<PipeColumnInfo> columns)
    {
        // Quote if needed or always quote for safety
        var fullTable = $"[{schema}].[{table}]";
        var sql = $"CREATE TABLE {fullTable} (";
        var cols = new List<string>();
        foreach (var col in columns)
        {
            string type = SqlServerTypeMapper.MapToProviderType(col.ClrType);
            cols.Add($"{SqlIdentifierHelper.GetSafeIdentifier(_dialect, col)} {type} NULL");
        }
        sql += string.Join(", ", cols) + ")";
        return sql;
    }

    private string BuildCreateTableFromIntrospection(string quotedTableName, TargetSchemaInfo schemaInfo)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE {quotedTableName} (");
        
        for (int i = 0; i < schemaInfo.Columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            var col = schemaInfo.Columns[i];
            
            // Quote identifier only if necessary
            var safeName = col.IsCaseSensitive || _dialect.NeedsQuoting(col.Name) ? _dialect.Quote(col.Name) : col.Name;
            
            var typeLower = col.NativeType.ToLowerInvariant();
            var fullType = col.NativeType;
            
            if (col.MaxLength.HasValue && (typeLower.Contains("char") || typeLower.Contains("binary")))
            {
                var lenStr = col.MaxLength.Value == -1 ? "MAX" : col.MaxLength.Value.ToString();
                fullType += $"({lenStr})";
            }
            else if (col.Precision.HasValue && col.Scale.HasValue && (typeLower == "decimal" || typeLower == "numeric"))
            {
                fullType += $"({col.Precision.Value},{col.Scale.Value})";
            }
            else if (col.Precision.HasValue && (typeLower.Contains("datetime2") || typeLower.Contains("datetimeoffset") || typeLower.Contains("time")))
            {
                fullType += $"({col.Precision.Value})";
            }

            sb.Append($"{safeName} {fullType}");
            
            if (!col.IsNullable)
            {
                sb.Append(" NOT NULL");
            }
        }
        
        // Add primary key constraint if present
        if (schemaInfo.PrimaryKeyColumns != null && schemaInfo.PrimaryKeyColumns.Count > 0)
        {
            sb.Append(", PRIMARY KEY (");
            for(int i=0; i < schemaInfo.PrimaryKeyColumns.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                sb.Append(_dialect.Quote(schemaInfo.PrimaryKeyColumns[i]));
            }
            sb.Append(")");
        }
        
        sb.Append(")");
        return sb.ToString();
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_bulkCopy == null || _bufferTable == null || _columns == null) throw new InvalidOperationException("Not initialized");

        if (_options.Strategy == SqlServerWriteStrategy.Upsert || _options.Strategy == SqlServerWriteStrategy.Ignore)
        {
             await ExecuteMergeAsync(rows, ct);
             return;
        }

        await ExecuteBulkInsertAsync(rows, ct);
    }

    private async Task ExecuteBulkInsertAsync(IEnumerable<object?[]> rows, CancellationToken ct)
    {
        _bufferTable!.Clear();
        foreach (var row in rows)
        {
            var dataRow = _bufferTable.NewRow();
            for (int i = 0; i < row.Length; i++)
            {
                dataRow[i] = row[i] ?? DBNull.Value;
            }
            _bufferTable.Rows.Add(dataRow);
        }
        
        await _bulkCopy!.WriteToServerAsync(_bufferTable, ct);
    }
    
    private async Task ExecuteMergeAsync(IReadOnlyList<object?[]> rows, CancellationToken ct)
    {
        // 1. Create Staging Table
        var stageTable = $"#Stage_{Guid.NewGuid():N}";

        // Create staging table by cloning target structure (SELECT INTO avoids constraints/indexes overhead)
        var createStageCmd = new SqlCommand($"SELECT TOP 0 * INTO [{stageTable}] FROM {_targetTableName}", _connection);
        await createStageCmd.ExecuteNonQueryAsync(ct);
        
        try
        {
            // 2. Bulk Copy to Stage
            using var stageBulk = new SqlBulkCopy(_connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null)
            {
                DestinationTableName = stageTable, 
                BatchSize = 0,
                BulkCopyTimeout = 0
            };
            
            // Map columns 1:1
            foreach (var col in _columns!)
            {
                stageBulk.ColumnMappings.Add(col.Name, col.Name);
            }
            
            _bufferTable!.Clear();
            foreach (var row in rows)
            {
                var dataRow = _bufferTable.NewRow();
                for (int i = 0; i < row.Length; i++)
                {
                    dataRow[i] = row[i] ?? DBNull.Value;
                }
                _bufferTable.Rows.Add(dataRow);
            }
            await stageBulk.WriteToServerAsync(_bufferTable, ct);
            
            // 3. Perform Merge
            var sb = new StringBuilder();
            sb.Append($"MERGE {_targetTableName} AS T ");
            sb.Append($"USING [{stageTable}] AS S ON (");
            
            for(int i=0; i<_keyColumns.Count; i++)
            {
                if (i>0) sb.Append(" AND ");
                // Keys link T and S
                var keyCol = _columns.First(c => c.Name.Equals(_keyColumns[i], StringComparison.OrdinalIgnoreCase));
                var safeKey = SqlIdentifierHelper.GetSafeIdentifier(_dialect, keyCol);
                sb.Append($"T.{safeKey} = S.[{_keyColumns[i]}]");
            }
            sb.Append(") ");
            
            if (_options.Strategy == SqlServerWriteStrategy.Upsert)
            {
                sb.Append("WHEN MATCHED THEN UPDATE SET ");
                var nonKeys = _columns.Where(c => !_keyColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase)).ToList();
                for(int i=0; i<nonKeys.Count; i++)
                {
                    if (i>0) sb.Append(", ");
                    var safeName = SqlIdentifierHelper.GetSafeIdentifier(_dialect, nonKeys[i]);
                    sb.Append($"T.{safeName} = S.[{nonKeys[i].Name}]");
                }
            }
            
            sb.Append(" WHEN NOT MATCHED THEN INSERT (");
            for(int i=0; i<_columns.Count; i++)
            {
                if (i>0) sb.Append(", ");
                sb.Append(SqlIdentifierHelper.GetSafeIdentifier(_dialect, _columns[i]));
            }
            sb.Append(") VALUES (");
            for(int i=0; i<_columns.Count; i++)
            {
                if (i>0) sb.Append(", ");
                sb.Append($"S.[{_columns[i].Name}]");
            }
            sb.Append(");"); // Merge requires semicolon
            
            var mergeCmd = new SqlCommand(sb.ToString(), _connection);
            mergeCmd.CommandTimeout = 0; // Infinite timeout for large merges
            await mergeCmd.ExecuteNonQueryAsync(ct);
        }
        finally
        {
            var dropCmd = new SqlCommand($"DROP TABLE IF EXISTS [{stageTable}]", _connection);
            await dropCmd.ExecuteNonQueryAsync(ct);
        }
    }
    
    // Original WriteBatchAsync replaced above

    public async ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
    {
        if (_connection == null)
        {
            _connection = new SqlConnection(_connectionString);
        }
        
        if (_connection.State != ConnectionState.Open)
        {
            await _connection.OpenAsync(ct);
        }

        using var cmd = new SqlCommand(command, _connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public ValueTask CompleteAsync(CancellationToken ct = default)
    {
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_bulkCopy != null) (_bulkCopy as IDisposable).Dispose();
        if (_connection != null) await _connection.DisposeAsync();
        if (_bufferTable != null) _bufferTable.Dispose();
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
            
        return _options.Key.Split(',')
            .Select(k => k.Trim())
            .Where(k => !string.IsNullOrEmpty(k))
            .ToList();
    }
    
    public bool RequiresPrimaryKey()
    {
        return _options.Strategy is 
            SqlServerWriteStrategy.Upsert or 
            SqlServerWriteStrategy.Ignore;
    }
}
