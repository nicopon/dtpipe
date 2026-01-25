using Microsoft.Data.SqlClient;
using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using System.Data;

namespace QueryDump.Adapters.SqlServer;

public class SqlServerDataWriter : IDataWriter
{
    private readonly string _connectionString;
    private readonly SqlServerWriterOptions _options;
    private SqlConnection? _connection;
    private SqlBulkCopy? _bulkCopy;
    private DataTable? _bufferTable;
    private long _bytesWritten;

    public long BytesWritten => _bytesWritten;

    public SqlServerDataWriter(string connectionString, SqlServerWriterOptions options)
    {
        _connectionString = connectionString;
        _options = options;
    }

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        _connection = new SqlConnection(_connectionString);
        await _connection.OpenAsync(ct);

        // Pre-action
        if (_options.Strategy == SqlServerWriteStrategy.Truncate)
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
            // Default Append: Check if table exists, create if not?
            // Simple Create Table Logic for testing convenience
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
            BatchSize = _options.BulkSize > 0 ? _options.BulkSize : 5000,
            BulkCopyTimeout = 0 // Infinite
        };

        // Initialize Buffer Table for Bulk Copy
        _bufferTable = new DataTable();
        foreach (var col in columns)
        {
            _bufferTable.Columns.Add(col.Name, Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType);
            _bulkCopy.ColumnMappings.Add(col.Name, col.Name);
        }
    }

    private string BuildCreateTableSql(string tableName, IReadOnlyList<ColumnInfo> columns)
    {
        var sql = $"CREATE TABLE [{tableName}] (";
        var cols = new List<string>();
        foreach (var col in columns)
        {
            string type = MapType(col.ClrType);
            cols.Add($"[{col.Name}] {type} NULL");
        }
        sql += string.Join(", ", cols) + ")";
        return sql;
    }

    private string MapType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        if (type == typeof(int)) return "INT";
        if (type == typeof(long)) return "BIGINT";
        if (type == typeof(short)) return "SMALLINT";
        if (type == typeof(string)) return "NVARCHAR(MAX)";
        if (type == typeof(DateTime)) return "DATETIME2";
        if (type == typeof(bool)) return "BIT";
        if (type == typeof(double)) return "FLOAT";
        if (type == typeof(decimal)) return "DECIMAL(18,2)";
        if (type == typeof(Guid)) return "UNIQUEIDENTIFIER";
        if (type == typeof(byte[])) return "VARBINARY(MAX)";
        return "NVARCHAR(MAX)";
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_bulkCopy == null || _bufferTable == null) throw new InvalidOperationException("Not initialized");

        _bufferTable.Clear();
        foreach (var row in rows)
        {
            var dataRow = _bufferTable.NewRow();
            for (int i = 0; i < row.Length; i++)
            {
                dataRow[i] = row[i] ?? DBNull.Value;
            }
            _bufferTable.Rows.Add(dataRow);
            
             // Approximation
             _bytesWritten += 100; // Minimal tracking
        }

        await _bulkCopy.WriteToServerAsync(_bufferTable, ct);
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
}
