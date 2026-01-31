using Microsoft.Data.SqlClient;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using System.Data;
using DtPipe.Core.Helpers;
using Microsoft.Extensions.Logging;
using System.Text;

namespace DtPipe.Adapters.SqlServer;

public class SqlServerDataWriter : IDataWriter, ISchemaInspector
{
    private readonly string _connectionString;
    private readonly SqlServerWriterOptions _options;
    private SqlConnection? _connection;
    private SqlBulkCopy? _bulkCopy;
    private DataTable? _bufferTable;

    private IReadOnlyList<ColumnInfo>? _columns;

    private List<string> _keyColumns = new();
    private List<int> _keyIndices = new();
    private BatchDiffProcessor? _diffProcessor;
    private ILogger<SqlServerDataWriter>? _logger; // Need Logger?

    // We can't change constructor signature easily without breaking signature compatibility 
    // BUT we need logger for DiffProcessor.
    // However, BatchDiffProcessor constructor takes ILogger (nullable?).
    // Yes, BatchDiffProcessor takes ILogger?.
    
    // Actually, I can instantiate BatchDiffProcessor with null logger if I don't have one.
    // The current class doesn't have a logger. I'll add one if possible, or proceed without.

    public SqlServerDataWriter(string connectionString, SqlServerWriterOptions options)
    {
        _connectionString = connectionString;
        _options = options;
        _diffProcessor = new BatchDiffProcessor(null); 
    }

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        _columns = columns;
        _connection = new SqlConnection(_connectionString);
        await _connection.OpenAsync(ct);

        // Pre-action
        if (_options.Strategy == SqlServerWriteStrategy.Recreate)
        {
            var cmd = new SqlCommand($"DROP TABLE IF EXISTS [{_options.Table}]", _connection);
            await cmd.ExecuteNonQueryAsync(ct);

            var createSql = BuildCreateTableSql(_options.Table, columns);
            var createCmd = new SqlCommand(createSql, _connection);
            await createCmd.ExecuteNonQueryAsync(ct);
        }
        else if (_options.Strategy == SqlServerWriteStrategy.Truncate)
        {
            var cmd = new SqlCommand($"TRUNCATE TABLE [{_options.Table}]", _connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        else if (_options.Strategy == SqlServerWriteStrategy.DeleteThenInsert)
        {
            var cmd = new SqlCommand($"DELETE FROM [{_options.Table}]", _connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        else
        {
            var checkCmd = new SqlCommand($"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{_options.Table}'", _connection);
            var exists = (int)(await checkCmd.ExecuteScalarAsync(ct) ?? 0) > 0;
            
            if (!exists)
            {
                var createSql = BuildCreateTableSql(_options.Table, columns);
                var createCmd = new SqlCommand(createSql, _connection);
                await createCmd.ExecuteNonQueryAsync(ct);
            }
        }

        // Configure SqlBulkCopy
        _bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null)
        {
            DestinationTableName = _options.Table,
            // BatchSize is controlled by pipeline buffer
            BatchSize = 0, 
            BulkCopyTimeout = 0
        };

        // Initialize Buffer Table for Bulk Copy
        _bufferTable = new DataTable();
        foreach (var col in columns)
        {
            _bufferTable.Columns.Add(col.Name, Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType);
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
    }

    #region ISchemaInspector Implementation

    public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        var tableName = _options.Table;

        // Check exists
        var checkCmd = new SqlCommand("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @Table", connection);
        checkCmd.Parameters.AddWithValue("@Table", tableName);
        var exists = (int)(await checkCmd.ExecuteScalarAsync(ct) ?? 0) > 0;

        if (!exists) return new TargetSchemaInfo([], false, null, null, null);

        // Get Columns
        var cols = new List<TargetColumnInfo>();
        var colCmd = new SqlCommand(@"
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = @Table
            ORDER BY ORDINAL_POSITION", connection);
        colCmd.Parameters.AddWithValue("@Table", tableName);

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

        // Get Primary Key
        var pkCmd = new SqlCommand(@"
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU ON TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
            WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.TABLE_NAME = @Table
            ORDER BY KCU.ORDINAL_POSITION", connection);
        pkCmd.Parameters.AddWithValue("@Table", tableName);
        
        var pkCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var pkReader = await pkCmd.ExecuteReaderAsync(ct);
        while(await pkReader.ReadAsync(ct))
        {
            pkCols.Add(pkReader.GetString(0));
        }

        return new TargetSchemaInfo(cols, true, null, null, pkCols.Count > 0 ? pkCols.ToList() : null);
    }

    #endregion

    private string BuildCreateTableSql(string tableName, IReadOnlyList<ColumnInfo> columns)
    {
        var sql = $"CREATE TABLE [{tableName}] (";
        var cols = new List<string>();
        foreach (var col in columns)
        {
            string type = SqlServerTypeMapper.MapToProviderType(col.ClrType);
            cols.Add($"[{col.Name}] {type} NULL");
        }
        sql += string.Join(", ", cols) + ")";
        return sql;
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_bulkCopy == null || _bufferTable == null || _columns == null) throw new InvalidOperationException("Not initialized");

        if (_options.Strategy == SqlServerWriteStrategy.Upsert || _options.Strategy == SqlServerWriteStrategy.Ignore)
        {
             // 1. Partition Batch
             var (newRows, existingRows) = await _diffProcessor!.PartitionBatchAsync(
                 rows, 
                 _keyIndices,
                 FetchExistingKeysAsync,
                 ct);
                 
             // 2. Insert New Rows
             if (newRows.Count > 0)
             {
                  await ExecuteBulkInsertAsync(newRows, ct);
             }
             
             // 3. Update Existing Rows (if Upsert)
             if (_options.Strategy == SqlServerWriteStrategy.Upsert && existingRows.Count > 0)
             {
                 await ExecuteBulkUpdateAsync(existingRows, ct);
             }
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
    
    private async Task ExecuteBulkUpdateAsync(List<object?[]> rows, CancellationToken ct)
    {
        // 1. Create Staging Table
        var stageTable = $"#Stage_{Guid.NewGuid():N}";
        var createStageCmd = new SqlCommand($"SELECT TOP 0 * INTO [{stageTable}] FROM [{_options.Table}]", _connection);
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
            
            // 3. Perform Update
            var sb = new StringBuilder();
            sb.Append($"UPDATE T SET ");
            
            var nonKeys = _columns.Where(c => !_keyColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase)).ToList();
            for(int i=0; i<nonKeys.Count; i++)
            {
                if (i>0) sb.Append(", ");
                sb.Append($"T.[{nonKeys[i].Name}] = S.[{nonKeys[i].Name}]");
            }
            
            sb.Append($" FROM [{_options.Table}] T INNER JOIN [{stageTable}] S ON ");
            for(int i=0; i<_keyColumns.Count; i++)
            {
                if (i>0) sb.Append(" AND ");
                sb.Append($"T.[{_keyColumns[i]}] = S.[{_keyColumns[i]}]");
            }
            
            var updateCmd = new SqlCommand(sb.ToString(), _connection);
            await updateCmd.ExecuteNonQueryAsync(ct);
        }
        finally
        {
            var dropCmd = new SqlCommand($"DROP TABLE IF EXISTS [{stageTable}]", _connection);
            await dropCmd.ExecuteNonQueryAsync(ct);
        }
    }
    
    private async Task<HashSet<string>> FetchExistingKeysAsync(IEnumerable<object[]> keys, CancellationToken ct)
    {
        var allFoundKeys = new HashSet<string>();
        // SQL Server max params ~2100. Chunk size 1000 is safe.
        var chunks = keys.Chunk(1000);
        
        foreach(var chunk in chunks)
        {
            var chunkList = chunk.ToList();
            if (chunkList.Count == 0) continue;

             var sb = new StringBuilder();
             sb.Append($"SELECT ");
             for(int i=0; i<_keyColumns.Count; i++) 
             {
                 if (i>0) sb.Append(", ");
                 sb.Append($"[{_keyColumns[i]}]");
             }
             sb.Append($" FROM [{_options.Table}] WHERE ");
             
             // Composite IN: (A,B) IN (VALUES (1,2), (3,4)) - Only modern SQL
             // Or (A=1 AND B=2) OR (A=3 AND B=4)
             // Cleanest for standard SQL Server: OR clauses
             
             for(int i=0; i<chunkList.Count; i++)
             {
                 if (i>0) sb.Append(" OR ");
                 sb.Append("(");
                 for(int k=0; k<_keyColumns.Count; k++)
                 {
                     if (k>0) sb.Append(" AND ");
                     sb.Append($"[{_keyColumns[k]}] = @k_{i}_{k}");
                 }
                 sb.Append(")");
             }
            
            using var cmd = new SqlCommand(sb.ToString(), _connection);
            for(int i=0; i<chunkList.Count; i++)
            {
                 for(int k=0; k<_keyColumns.Count; k++)
                 {
                     cmd.Parameters.AddWithValue($"@k_{i}_{k}", chunkList[i][k] ?? DBNull.Value);
                 }
            }
            
            using var reader = await cmd.ExecuteReaderAsync(ct);
            while(await reader.ReadAsync(ct))
            {
                var values = new object[_keyColumns.Count];
                reader.GetValues(values);
                allFoundKeys.Add(BatchDiffProcessor.GenerateKeyString(values));
            }
        }
        return allFoundKeys;
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
}
