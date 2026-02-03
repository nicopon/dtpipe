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

    private IReadOnlyList<ColumnInfo>? _columns;

    private string _targetTableName = ""; // Resolved or Fallback
    private List<string> _keyColumns = new();
    
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
        // Quote only if necessary (special chars, reserved words, mixed case)
        // Standard simple identifiers (alphanumeric, underscore, uppercase/lowercase only) don't need quoting
        return _dialect.NeedsQuoting(identifier) ? _dialect.Quote(identifier) : identifier;
    }
    private List<int> _keyIndices = new();

    private readonly ISqlDialect _dialect = new DtPipe.Core.Dialects.SqlServerDialect();
    public ISqlDialect Dialect => _dialect;

    public SqlServerDataWriter(string connectionString, SqlServerWriterOptions options)
    {
        _connectionString = connectionString;
        _options = options;
    }

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        // GLOBAL NORMALIZATION:
        // Create a secure list of columns where names are normalized if not case-sensitive.
        // This ensures consistency across CREATE TABLE, INSERT, BULK COPY, MERGE, etc.
        var normalizedColumns = new List<ColumnInfo>(columns.Count);
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
        _connection = new SqlConnection(_connectionString);
        await _connection.OpenAsync(ct);

        // Native Resolution logic
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
            // DROP uses quoted name
            var cmd = new SqlCommand($"DROP TABLE IF EXISTS {_targetTableName}", _connection);
            await cmd.ExecuteNonQueryAsync(ct);

            // Create using canonical names
            var createSql = BuildCreateTableSql(resolvedSchema, resolvedTable, columns);
            var createCmd = new SqlCommand(createSql, _connection);
            await createCmd.ExecuteNonQueryAsync(ct);
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
        
        // Configure SqlBulkCopy
        _bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null)
        {
            DestinationTableName = _targetTableName,
            // BatchSize is controlled by pipeline buffer
            BatchSize = 0, 
            BulkCopyTimeout = 0
        };

        // Initialize Buffer Table for Bulk Copy
        _bufferTable = new DataTable();
        foreach (var col in columns)
        {
            var type = Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType;
            // DataTable needs Base Type for nullable columns? Or just Type?
            // "Nullable types are not supported in DataTable" - traditionally.
            // Correct approach: Use base type and AllowDBNull = true.
            _bufferTable.Columns.Add(col.Name, type); 
            
            // Map Source (in buffer) to Dest (in DB)
            // Buffer col name is col.Name.
            // Dest col name is col.Name.
            // Smart quoting applies to SQL text, not Mapping names usually.
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

        // Get Columns
        var cols = new List<TargetColumnInfo>();
        {
            var colCmd = new SqlCommand(@"
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
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
                
                cols.Add(new TargetColumnInfo(
                    name,
                    type,
                    SqlServerTypeMapper.MapFromProviderType(type),
                    nullable,
                    false,
                    false,
                    null
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

        // Get Unique Columns (Phase 3)
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
    private string BuildCreateTableSql(string schema, string table, IReadOnlyList<ColumnInfo> columns)
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

    // IKeyValidator implementation (Phase 1)
    
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
