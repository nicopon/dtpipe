using System.Text;
using Microsoft.Data.Sqlite;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.Sqlite;

public class SqliteDataWriter : IDataWriter
{
    private readonly string _connectionString;
    private readonly OptionsRegistry _registry;

    private SqliteConnection? _connection;
    private IReadOnlyList<ColumnInfo>? _columns;
    private string _tableName = "Export";
    private string _strategy = "Append";

    public long BytesWritten { get; private set; }

    public SqliteDataWriter(string connectionString, OptionsRegistry registry)
    {
        _connectionString = connectionString;
        _registry = registry;
    }

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        _columns = columns;

        var options = _registry.Get<SqliteWriterOptions>();
        _tableName = options.Table;
        _strategy = options.Strategy;

        _connection = new SqliteConnection(_connectionString);
        await _connection.OpenAsync(ct);

        await HandleStrategyAsync(ct);
    }

    private async Task HandleStrategyAsync(CancellationToken ct)
    {
        using var cmd = _connection!.CreateCommand();

        if (_strategy.Equals("Recreate", StringComparison.OrdinalIgnoreCase))
        {
            cmd.CommandText = $"DROP TABLE IF EXISTS \"{_tableName}\"";
            await cmd.ExecuteNonQueryAsync(ct);
            await CreateTableAsync(ct);
        }
        else if (_strategy.Equals("Truncate", StringComparison.OrdinalIgnoreCase))
        {
            cmd.CommandText = $"SELECT name FROM sqlite_master WHERE type='table' AND name='{_tableName}'";
            var exists = await cmd.ExecuteScalarAsync(ct) != null;
            
            if (exists)
            {
                cmd.CommandText = $"DELETE FROM \"{_tableName}\"";
                await cmd.ExecuteNonQueryAsync(ct);
            }
            else
            {
                await CreateTableAsync(ct);
            }
        }
        else // Append
        {
            cmd.CommandText = $"SELECT name FROM sqlite_master WHERE type='table' AND name='{_tableName}'";
            var exists = await cmd.ExecuteScalarAsync(ct) != null;
            
            if (!exists)
            {
                await CreateTableAsync(ct);
            }
        }
    }

    private async Task CreateTableAsync(CancellationToken ct)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE \"{_tableName}\" (");

        for (int i = 0; i < _columns!.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            var col = _columns[i];
            sb.Append($"\"{col.Name}\" {SqliteTypeMapper.MapClrType(col.ClrType)}");
        }

        sb.Append(')');

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (rows.Count == 0) return;

        using var transaction = _connection!.BeginTransaction();

        var paramNames = string.Join(", ", Enumerable.Range(0, _columns!.Count).Select(i => $"@p{i}"));
        var columnNames = string.Join(", ", _columns.Select(c => $"\"{c.Name}\""));

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"INSERT INTO \"{_tableName}\" ({columnNames}) VALUES ({paramNames})";
        cmd.Transaction = transaction;

        // Create parameters once
        for (int i = 0; i < _columns.Count; i++)
        {
            cmd.Parameters.Add(new SqliteParameter($"@p{i}", null));
        }

        foreach (var row in rows)
        {
            for (int i = 0; i < _columns.Count; i++)
            {
                cmd.Parameters[i].Value = row[i] ?? DBNull.Value;
            }
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await transaction.CommitAsync(ct);

        // Estimate bytes written (rough approximation)
        BytesWritten += rows.Count * _columns.Count * 8; // Rough estimate
    }

    public ValueTask CompleteAsync(CancellationToken ct = default)
    {
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection != null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }
}
