using System.Text;
using Npgsql;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.PostgreSQL;

public sealed class PostgreSqlDataWriter : IDataWriter
{
    private readonly string _connectionString;
    private readonly PostgreSqlOptions _options;
    private NpgsqlConnection? _connection;
    private NpgsqlBinaryImporter? _writer;
    private long _bytesWritten;

    public long BytesWritten => _bytesWritten;

    public PostgreSqlDataWriter(string connectionString, PostgreSqlOptions options)
    {
        _connectionString = connectionString;
        _options = options;
    }

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        _connection = new NpgsqlConnection(_connectionString);
        await _connection.OpenAsync(ct);

        // Handle Strategy (Create/Truncate/Append)
        if (_options.Strategy == PostgreSqlWriteStrategy.Recreate)
        {
            await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS \"{_options.Table}\"", ct);
        }

        // Create table if not exists
        var createSql = BuildCreateTableSql(_options.Table, columns);
        await ExecuteNonQueryAsync(createSql, ct);
        
        if (_options.Strategy == PostgreSqlWriteStrategy.Truncate)
        {
            await ExecuteNonQueryAsync($"TRUNCATE TABLE \"{_options.Table}\"", ct);
        }

        // Begin Binary Import
        // Construct COPY command
        var copySql = BuildCopySql(_options.Table, columns);
        _writer = await _connection.BeginBinaryImportAsync(copySql, ct);
    }
    
    private async Task ExecuteNonQueryAsync(string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, _connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_writer is null) throw new InvalidOperationException("Writer not initialized");

        foreach (var row in rows)
        {
            await _writer.StartRowAsync(ct);
            foreach (var val in row)
            {
                if (val is null)
                {
                    await _writer.WriteNullAsync(ct);
                }
                else
                {
                    await _writer.WriteAsync(val, ct);
                }
            }
            // Rough estimation of bytes (row overhead + data)
            _bytesWritten += 8 + row.Sum(o => o?.ToString()?.Length ?? 0);
        }
    }

    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        if (_writer != null)
        {
            await _writer.CompleteAsync(ct);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_writer != null)
        {
            await _writer.DisposeAsync();
        }
        if (_connection != null)
        {
            await _connection.DisposeAsync();
        }
    }

    private string BuildCreateTableSql(string tableName, IReadOnlyList<ColumnInfo> columns)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE IF NOT EXISTS \"{tableName}\" (");
        
        for (int i = 0; i < columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            // Use double quotes for column names to handle case sensitivity and keywords
            sb.Append($"\"{columns[i].Name}\" {PostgreSqlTypeMapper.Instance.MapClrType(columns[i].ClrType)}");
        }
        
        sb.Append(")");
        return sb.ToString();
    }

    private string BuildCopySql(string tableName, IReadOnlyList<ColumnInfo> columns)
    {
        var sb = new StringBuilder();
        sb.Append($"COPY \"{tableName}\" (");
        for (int i = 0; i < columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"\"{columns[i].Name}\"");
        }
        sb.Append(") FROM STDIN (FORMAT BINARY)");
        return sb.ToString();
    }
}
