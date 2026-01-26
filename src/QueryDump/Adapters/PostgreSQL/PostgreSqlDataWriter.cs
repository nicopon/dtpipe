using System.Text;
using System.Text.RegularExpressions;
using Npgsql;
using QueryDump.Core.Abstractions;
using QueryDump.Cli.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Core.Options;
using QueryDump.Core.Helpers;

namespace QueryDump.Adapters.PostgreSQL;

public sealed partial class PostgreSqlDataWriter : IDataWriter, ISchemaInspector
{
    private readonly string _connectionString;
    private readonly PostgreSqlWriterOptions _options;
    private NpgsqlConnection? _connection;
    private NpgsqlBinaryImporter? _writer;
    private long _bytesWritten;
    private IReadOnlyList<ColumnInfo>? _columns;


    public long BytesWritten => _bytesWritten;

    public PostgreSqlDataWriter(string connectionString, PostgreSqlWriterOptions options)
    {
        _connectionString = connectionString;
        _options = options;
    }

    #region ISchemaInspector Implementation

    public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        var (schemaName, tableName) = ParseTableName(_options.Table);

        // Check if table exists
        var existsSql = @"
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = @schema AND table_name = @table";
        
        await using var existsCmd = new NpgsqlCommand(existsSql, connection);
        existsCmd.Parameters.AddWithValue("schema", schemaName);
        existsCmd.Parameters.AddWithValue("table", tableName);
        
        var exists = Convert.ToInt64(await existsCmd.ExecuteScalarAsync(ct)) > 0;
        if (!exists)
        {
            return new TargetSchemaInfo([], false, null, null, null);
        }

        // Get columns
        var columnsSql = @"
            SELECT 
                column_name, 
                data_type,
                udt_name,
                is_nullable,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns 
            WHERE table_schema = @schema AND table_name = @table
            ORDER BY ordinal_position";
        
        await using var columnsCmd = new NpgsqlCommand(columnsSql, connection);
        columnsCmd.Parameters.AddWithValue("schema", schemaName);
        columnsCmd.Parameters.AddWithValue("table", tableName);

        // Get primary key columns
        var pkSql = @"
            SELECT a.attname
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE c.contype = 'p' 
              AND c.conrelid = (SELECT oid FROM pg_class WHERE relname = @table 
                  AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = @schema))";
        
        var pkColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using (var pkCmd = new NpgsqlCommand(pkSql, connection))
        {
            pkCmd.Parameters.AddWithValue("schema", schemaName);
            pkCmd.Parameters.AddWithValue("table", tableName);
            await using var pkReader = await pkCmd.ExecuteReaderAsync(ct);
            while (await pkReader.ReadAsync(ct))
            {
                pkColumns.Add(pkReader.GetString(0));
            }
        }

        // Get unique constraint columns
        var uniqueSql = @"
            SELECT a.attname
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE c.contype = 'u' 
              AND c.conrelid = (SELECT oid FROM pg_class WHERE relname = @table 
                  AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = @schema))";
        
        var uniqueColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using (var uniqueCmd = new NpgsqlCommand(uniqueSql, connection))
        {
            uniqueCmd.Parameters.AddWithValue("schema", schemaName);
            uniqueCmd.Parameters.AddWithValue("table", tableName);
            await using var uniqueReader = await uniqueCmd.ExecuteReaderAsync(ct);
            while (await uniqueReader.ReadAsync(ct))
            {
                uniqueColumns.Add(uniqueReader.GetString(0));
            }
        }

        // Get row count and size
        var statsSql = @"
            SELECT 
                (SELECT reltuples::bigint FROM pg_class WHERE relname = @table 
                    AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = @schema)) as row_count,
                (SELECT pg_total_relation_size((@schema || '.' || @table)::regclass)) as size_bytes";
        
        long? rowCount = null;
        long? sizeBytes = null;
        await using (var statsCmd = new NpgsqlCommand(statsSql, connection))
        {
            statsCmd.Parameters.AddWithValue("schema", schemaName);
            statsCmd.Parameters.AddWithValue("table", tableName);
            await using var statsReader = await statsCmd.ExecuteReaderAsync(ct);
            if (await statsReader.ReadAsync(ct))
            {
                rowCount = statsReader.IsDBNull(0) ? null : statsReader.GetInt64(0);
                sizeBytes = statsReader.IsDBNull(1) ? null : statsReader.GetInt64(1);
            }
        }

        // Build column list
        var columns = new List<TargetColumnInfo>();
        await using var reader = await columnsCmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var colName = reader.GetString(0);
            var dataType = reader.GetString(1);
            var udtName = reader.GetString(2);
            var isNullable = reader.GetString(3) == "YES";
            var maxLength = reader.IsDBNull(4) ? (int?)null : reader.GetInt32(4);

            var nativeType = BuildNativeType(dataType, udtName, maxLength, 
                reader.IsDBNull(5) ? null : reader.GetInt32(5),
                reader.IsDBNull(6) ? null : reader.GetInt32(6));

            columns.Add(new TargetColumnInfo(
                colName,
                nativeType,
                MapPostgresToClr(dataType, udtName),
                isNullable,
                pkColumns.Contains(colName),
                uniqueColumns.Contains(colName),
                maxLength
            ));
        }

        return new TargetSchemaInfo(
            columns,
            true,
            rowCount >= 0 ? rowCount : null, // negative means stats not available
            sizeBytes,
            pkColumns.Count > 0 ? pkColumns.ToList() : null
        );
    }

    private static (string schema, string table) ParseTableName(string fullName)
    {
        // Remove quotes and parse schema.table
        var cleaned = fullName.Replace("\"", "");
        var parts = cleaned.Split('.');
        return parts.Length == 2 
            ? (parts[0], parts[1]) 
            : ("public", parts[0]);
    }

    private static string BuildNativeType(string dataType, string udtName, int? maxLength, int? precision, int? scale)
    {
        return dataType.ToUpperInvariant() switch
        {
            "character varying" when maxLength.HasValue => $"VARCHAR({maxLength})",
            "character varying" => "VARCHAR",
            "character" when maxLength.HasValue => $"CHAR({maxLength})",
            "character" => "CHAR",
            "numeric" when precision.HasValue && scale.HasValue => $"NUMERIC({precision},{scale})",
            "numeric" when precision.HasValue => $"NUMERIC({precision})",
            "numeric" => "NUMERIC",
            _ => udtName.ToUpperInvariant()
        };
    }

    private static Type? MapPostgresToClr(string dataType, string udtName)
    {
        return udtName.ToLowerInvariant() switch
        {
            "int2" => typeof(short),
            "int4" => typeof(int),
            "int8" => typeof(long),
            "float4" => typeof(float),
            "float8" => typeof(double),
            "numeric" => typeof(decimal),
            "bool" => typeof(bool),
            "text" or "varchar" or "bpchar" => typeof(string),
            "timestamp" or "timestamptz" => typeof(DateTime),
            "date" => typeof(DateTime),
            "time" or "timetz" => typeof(TimeSpan),
            "uuid" => typeof(Guid),
            "bytea" => typeof(byte[]),
            _ => typeof(string) // Default to string for unknown types
        };
    }

    #endregion

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        _connection = new NpgsqlConnection(_connectionString);
        await _connection.OpenAsync(ct);
        _columns = columns;

        // Handle Strategy (Create/Truncate/Append/Delete)
        if (_options.Strategy == PostgreSqlWriteStrategy.DeleteThenInsert)
        {
            await ExecuteNonQueryAsync($"DELETE FROM \"{_options.Table}\"", ct);
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
        if (_columns is null) throw new InvalidOperationException("Columns not initialized");

        try
        {
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
        catch (Exception ex)
        {
             var analysis = await BatchFailureAnalyzer.AnalyzeAsync(this, rows, _columns, ct);
             if (!string.IsNullOrEmpty(analysis))
            {
                throw new InvalidOperationException($"PostgreSQL Binary Import Failed with detailed analysis:\n{analysis}", ex);
            }
            throw;
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
