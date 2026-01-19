using Oracle.ManagedDataAccess.Client;
using QueryDump.Core;
using QueryDump.Core.Options;
using System.Data;
using System.Text;

namespace QueryDump.Providers.Oracle;

public sealed class OracleDataWriter : IDataWriter
{
    private readonly string _connectionString;
    private readonly OracleConnection _connection;
    private readonly OracleWriterOptions _options;
    private IReadOnlyList<ColumnInfo>? _columns;

    public long BytesWritten => 0;

    public OracleDataWriter(string connectionString, OracleWriterOptions options)
    {
        _connectionString = connectionString;
        _options = options;
        _connection = new OracleConnection(connectionString);
    }

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        _columns = columns;
        await _connection.OpenAsync(ct);

        if (_options.Strategy == OracleWriteStrategy.Recreate)
        {
            // Drop Table logic
            try {
                using var dropCmd = _connection.CreateCommand();
                dropCmd.CommandText = $"DROP TABLE \"{_options.Table}\"";
                await dropCmd.ExecuteNonQueryAsync(ct);
            } catch (OracleException ex) when (ex.Number == 942) { /* ORA-00942: table or view does not exist */ }
        }
        else if (_options.Strategy == OracleWriteStrategy.Truncate)
        {
            // Truncate logic
             try {
                using var truncCmd = _connection.CreateCommand();
                truncCmd.CommandText = $"TRUNCATE TABLE \"{_options.Table}\"";
                await truncCmd.ExecuteNonQueryAsync(ct);
            } catch (OracleException ex) when (ex.Number == 942) { /* Table doesn't exist, ignore */ }
        }

        var createTableSql = BuildCreateTableSql(_options.Table, columns);
        
        try
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = createTableSql;
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (OracleException ex) when (ex.Number == 955) // ORA-00955: name already used
        {
            // Expected if table exists (Append/Truncate modes)
        }
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_columns is null) throw new InvalidOperationException("Not initialized");

        using var bulkCopy = new OracleBulkCopy(_connection);
        bulkCopy.DestinationTableName = _options.Table;
        bulkCopy.BulkCopyTimeout = 0; // Infinite (or config?)
        bulkCopy.BatchSize = _options.BulkSize;

        // Convert rows to DataTable or IDataReader
        using var dataTable = new DataTable();
        foreach (var col in _columns)
        {
            dataTable.Columns.Add(col.Name, Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType);
        }

        foreach (var rowData in rows)
        {
            var row = dataTable.NewRow();
            for (int i = 0; i < rowData.Length; i++)
            {
                row[i] = rowData[i] ?? DBNull.Value;
            }
            dataTable.Rows.Add(row);
        }

        await Task.Run(() => bulkCopy.WriteToServer(dataTable), ct);
    }

    public ValueTask CompleteAsync(CancellationToken ct = default)
    {
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync();
    }

    private string BuildCreateTableSql(string tableName, IReadOnlyList<ColumnInfo> columns)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE {tableName} (");
        
        for (int i = 0; i < columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            // Oracle max identifier length matters (30 bytes in <12.2, 128 in 12.2+)
            // We assume sensible names.
            sb.Append($"\"{columns[i].Name}\" {MapToOracleType(columns[i].ClrType)}");
        }
        
        sb.Append(")");
        return sb.ToString();
    }

    private static string MapToOracleType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;

        if (type == typeof(int)) return "NUMBER(10)";
        if (type == typeof(long)) return "NUMBER(19)";
        if (type == typeof(short)) return "NUMBER(5)";
        if (type == typeof(byte)) return "NUMBER(3)";
        if (type == typeof(bool)) return "NUMBER(1)"; // Oracle has no BOOLEAN in SQL
        if (type == typeof(float)) return "FLOAT"; // or BINARY_FLOAT
        if (type == typeof(double)) return "FLOAT"; // or BINARY_DOUBLE
        if (type == typeof(decimal)) return "NUMBER(38, 4)"; // Default precision?
        if (type == typeof(DateTime)) return "TIMESTAMP";
        if (type == typeof(DateTimeOffset)) return "TIMESTAMP WITH TIME ZONE";
        if (type == typeof(Guid)) return "RAW(16)";
        if (type == typeof(byte[])) return "BLOB";
        
        return "VARCHAR2(4000)";
    }
}
