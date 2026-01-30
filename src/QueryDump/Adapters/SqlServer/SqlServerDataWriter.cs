using Microsoft.Data.SqlClient;
using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using System.Data;
using QueryDump.Core.Helpers;

namespace QueryDump.Adapters.SqlServer;

public class SqlServerDataWriter : IDataWriter, ISchemaInspector
{
    private readonly string _connectionString;
    private readonly SqlServerWriterOptions _options;
    private SqlConnection? _connection;
    private SqlBulkCopy? _bulkCopy;
    private DataTable? _bufferTable;

    private IReadOnlyList<ColumnInfo>? _columns;

    public SqlServerDataWriter(string connectionString, SqlServerWriterOptions options)
    {
        _connectionString = connectionString;
        _options = options;
    }

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        _columns = columns;
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

        return new TargetSchemaInfo(cols, true, null, null, null);
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

        _bufferTable.Clear();
        foreach (var row in rows)
        {
            var dataRow = _bufferTable.NewRow();
            for (int i = 0; i < row.Length; i++)
            {
                dataRow[i] = row[i] ?? DBNull.Value;
            }
            _bufferTable.Rows.Add(dataRow);
        }

        try
        {
            await _bulkCopy.WriteToServerAsync(_bufferTable, ct);
        }
        catch (Exception ex)
        {
            var analysis = await BatchFailureAnalyzer.AnalyzeAsync(this, rows, _columns, ct);
             if (!string.IsNullOrEmpty(analysis))
            {
                throw new InvalidOperationException($"SqlBulkCopy Failed with detailed analysis:\n{analysis}", ex);
            }
            throw;
        }
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
