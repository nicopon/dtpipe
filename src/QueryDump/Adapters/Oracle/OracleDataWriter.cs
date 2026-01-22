using Oracle.ManagedDataAccess.Client;
using QueryDump.Core;
using QueryDump.Core.Options;
using System.Data;
using System.Text;

namespace QueryDump.Adapters.Oracle;

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

    private string _targetTableName = "";

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        var targetTable = NormalizeTableName(_options.Table);
        Console.WriteLine($"[OracleDataWriter] Target Table (Raw): '{_options.Table}' -> Normalized: '{targetTable}'");
        
        _targetTableName = targetTable;
        _columns = columns;
        await _connection.OpenAsync(ct);

        if (_options.Strategy == OracleWriteStrategy.DeleteThenInsert)
        {
            try {
                using var deleteCmd = _connection.CreateCommand();
                deleteCmd.CommandText = $"DELETE FROM {_targetTableName}";
                await deleteCmd.ExecuteNonQueryAsync(ct);
            } catch (OracleException ex) when (ex.Number == 942) { /* ORA-00942: table or view does not exist */ }
        }
        else if (_options.Strategy == OracleWriteStrategy.Truncate)
        {
             try {
                using var truncCmd = _connection.CreateCommand();
                truncCmd.CommandText = $"TRUNCATE TABLE {_targetTableName}";
                await truncCmd.ExecuteNonQueryAsync(ct);
            } catch (OracleException ex) when (ex.Number == 942) { /* Table doesn't exist, ignore */ }
        }

        var createTableSql = BuildCreateTableSql(_targetTableName, columns);
        
        try
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = createTableSql;
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (OracleException ex) when (ex.Number == 955) // ORA-00955: name already used
        {
            // Expected if table exists
        }
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_columns is null) throw new InvalidOperationException("Not initialized");

        if (_options.BulkSize <= 0)
        {
            // Standard INSERT fallback
            // Build parameterized INSERT statement
            var sb = new StringBuilder();
            sb.Append($"INSERT INTO {_targetTableName} (");
            for (int i = 0; i < _columns.Count; i++)
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
Console.WriteLine($"[OracleDataWriter] Insert SQL: {insertSql}");
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = insertSql;
            // Pre-create parameters
            var parameters = new OracleParameter[_columns.Count];
            for (int i = 0; i < _columns.Count; i++)
            {
                parameters[i] = cmd.CreateParameter();
                parameters[i].ParameterName = $"v{i}";
                // Map types if needed or let inference handle simple types
                cmd.Parameters.Add(parameters[i]);
            }

            foreach (var rowData in rows)
            {
                for (int i = 0; i < rowData.Length; i++)
                {
                   parameters[i].Value = rowData[i] ?? DBNull.Value;
                }
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }
        else
        {
            // Existing BulkCopy logic
            using var bulkCopy = new OracleBulkCopy(_connection);
            bulkCopy.DestinationTableName = _targetTableName;
            bulkCopy.BulkCopyTimeout = 0; 
            bulkCopy.BatchSize = _options.BulkSize;

            // Use IDataReader wrapper instead of DataTable for better performance
            using var dataReader = new ObjectArrayDataReader(_columns, rows);
            await Task.Run(() => bulkCopy.WriteToServer(dataReader), ct);
        }
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
        if (type == typeof(bool)) return "NUMBER(1)"; 
        if (type == typeof(float)) return "FLOAT"; 
        if (type == typeof(double)) return "FLOAT"; 
        if (type == typeof(decimal)) return "NUMBER(38, 4)"; 
        if (type == typeof(DateTime)) return "TIMESTAMP";
        if (type == typeof(DateTimeOffset)) return "TIMESTAMP WITH TIME ZONE";
        if (type == typeof(Guid)) return "RAW(16)";
        if (type == typeof(byte[])) return "BLOB";
        
        return "VARCHAR2(4000)";
    }


    private static string NormalizeTableName(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName)) return tableName;

        var parts = tableName.Split('.');
        for (int i = 0; i < parts.Length; i++)
        {
            var part = parts[i];
            if (!part.StartsWith("\"") && !part.EndsWith("\""))
            {
                // Unquoted identifier: normalize to uppercase and quote
                parts[i] = $"\"{part.ToUpperInvariant()}\"";
            }
            // Else: verify if it's properly quoted? 
            // For now assume if user quoted it, they know what they are doing.
        }
        
        return string.Join(".", parts);
    }
}
