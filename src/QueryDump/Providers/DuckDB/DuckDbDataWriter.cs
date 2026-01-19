using DuckDB.NET.Data;
using QueryDump.Core;
using QueryDump.Core.Options;
using System.Text;
using ColumnInfo = QueryDump.Core.ColumnInfo;

namespace QueryDump.Providers.DuckDB;

public sealed class DuckDbDataWriter : IDataWriter
{
    private readonly string _connectionString;
    private readonly DuckDBConnection _connection;
    private readonly DuckDbWriterOptions _options;
    private IReadOnlyList<QueryDump.Core.ColumnInfo>? _columns;

    public long BytesWritten => 0; 

    public DuckDbDataWriter(string connectionString, DuckDbWriterOptions options)
    {
        _connectionString = connectionString;
        _options = options;
        _connection = new DuckDBConnection(connectionString);
    }

    public async ValueTask InitializeAsync(IReadOnlyList<QueryDump.Core.ColumnInfo> columns, CancellationToken ct = default)
    {
        _columns = columns;
        await _connection.OpenAsync(ct);

        if (_options.Strategy == DuckDbWriteStrategy.Recreate)
        {
            var dropCmd = _connection.CreateCommand();
            dropCmd.CommandText = $"DROP TABLE IF EXISTS {_options.Table}";
            await dropCmd.ExecuteNonQueryAsync(ct);
        }
        else if (_options.Strategy == DuckDbWriteStrategy.Truncate)
        {
            try {
                var truncCmd = _connection.CreateCommand();
                truncCmd.CommandText = $"DELETE FROM {_options.Table}";
                await truncCmd.ExecuteNonQueryAsync(ct);
            } catch { /* Ignore if table does not exist */ }
        }

        var createTableSql = BuildCreateTableSql(_options.Table, columns);
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = createTableSql;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_columns is null) throw new InvalidOperationException("Not initialized");

        using var appender = _connection.CreateAppender(_options.Table);
        
        foreach (var rowData in rows)
        {
            var row = appender.CreateRow();
            for (int i = 0; i < rowData.Length; i++)
            {
                var val = rowData[i];
                var col = _columns[i];
                
                if (val is null)
                {
                    row.AppendNullValue();
                }
                else
                {
                    AppendValue(row, val, col.ClrType);
                }
            }
            row.EndRow();
        }
    }

    private void AppendValue(IDuckDBAppenderRow row, object val, Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;

        if (underlying == typeof(int)) row.AppendValue((int)val);
        else if (underlying == typeof(long)) row.AppendValue((long)val);
        else if (underlying == typeof(short)) row.AppendValue((short)val);
        else if (underlying == typeof(byte)) row.AppendValue((byte)val);
        else if (underlying == typeof(bool)) row.AppendValue((bool)val);
        else if (underlying == typeof(float)) row.AppendValue((float)val);
        else if (underlying == typeof(double)) row.AppendValue((double)val);
        else if (underlying == typeof(decimal)) row.AppendValue((decimal)val);
        else if (underlying == typeof(DateTime)) row.AppendValue((DateTime)val);
        else if (underlying == typeof(DateTimeOffset)) row.AppendValue((DateTimeOffset)val);
        else if (underlying == typeof(Guid)) row.AppendValue((Guid)val);
        else if (underlying == typeof(byte[])) row.AppendValue((byte[])val);
        else row.AppendValue(val.ToString());
    }

    public ValueTask CompleteAsync(CancellationToken ct = default)
    {
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync();
    }

    private string BuildCreateTableSql(string tableName, IReadOnlyList<QueryDump.Core.ColumnInfo> columns)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE IF NOT EXISTS {tableName} (");
        
        for (int i = 0; i < columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"{columns[i].Name} {MapToDuckDbType(columns[i].ClrType)}");
        }
        
        sb.Append(")");
        return sb.ToString();
    }

    private static string MapToDuckDbType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;

        if (type == typeof(int)) return "INTEGER";
        if (type == typeof(long)) return "BIGINT";
        if (type == typeof(short)) return "SMALLINT";
        if (type == typeof(byte)) return "TINYINT";
        if (type == typeof(bool)) return "BOOLEAN";
        if (type == typeof(float)) return "FLOAT";
        if (type == typeof(double)) return "DOUBLE";
        if (type == typeof(decimal)) return "DECIMAL";
        if (type == typeof(DateTime)) return "TIMESTAMP";
        if (type == typeof(DateTimeOffset)) return "TIMESTAMP";
        if (type == typeof(Guid)) return "UUID";
        if (type == typeof(byte[])) return "BLOB";
        
        return "VARCHAR";
    }
}
