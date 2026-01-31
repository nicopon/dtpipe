using System.Text;
using System.Text.RegularExpressions;
using Npgsql;
using DtPipe.Core.Abstractions;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Helpers;

namespace DtPipe.Adapters.PostgreSQL;

public sealed partial class PostgreSqlDataWriter : IDataWriter, ISchemaInspector
{
    private readonly string _connectionString;
    private readonly PostgreSqlWriterOptions _options;
    private NpgsqlConnection? _connection;
    private NpgsqlBinaryImporter? _writer;
    private string? _stagingTable;
    private List<string> _keyColumns = new();

    private IReadOnlyList<ColumnInfo>? _columns;

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
                PostgreSqlTypeMapper.Instance.MapFromProviderType(udtName),
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



    #endregion

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        _connection = new NpgsqlConnection(_connectionString);
        await _connection.OpenAsync(ct);
        _columns = columns;

        // Handle Strategy (Create/Truncate/Append/Delete)
        if (_options.Strategy == PostgreSqlWriteStrategy.Recreate)
        {
            await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS \"{_options.Table}\"", ct);
        }
        else if (_options.Strategy == PostgreSqlWriteStrategy.DeleteThenInsert)
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

        // Incremental Loading Setup
        string targetTable = _options.Table;

        if (_options.Strategy == PostgreSqlWriteStrategy.Upsert || _options.Strategy == PostgreSqlWriteStrategy.Ignore)
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
                  throw new InvalidOperationException($"Strategy {_options.Strategy} requires a Primary Key. None detected. Please ensure the target table has a primary key.");
            }
             
            // 2. Create Staging Table (TEMP table)
            _stagingTable = $"tmp_stage_{Guid.NewGuid():N}";
            // Remove ON COMMIT DROP because in autocommit mode it drops immediately.
            // We explicit drop in CompleteAsync anyway.
            await ExecuteNonQueryAsync($"CREATE TEMP TABLE \"{_stagingTable}\" (LIKE \"{_options.Table}\" INCLUDING DEFAULTS)", ct);
            
            targetTable = _stagingTable;
        }

        // Begin Binary Import
        // Construct COPY command
        var copySql = BuildCopySql(targetTable, columns);
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
            await _writer.DisposeAsync(); 
            _writer = null;

            // Perform Merge if Staging
            if (_stagingTable != null)
            {
                var cols = _columns!.Select(c => $"\"{c.Name}\"").ToList();
                var conflictTarget = string.Join(", ", _keyColumns.Select(c => $"\"{c}\""));
                var updateSet = string.Join(", ", cols.Where(c => !_keyColumns.Contains(c.Replace("\"", ""), StringComparer.OrdinalIgnoreCase))
                                                      .Select(c => $"{c} = EXCLUDED.{c}"));

                var sb = new StringBuilder();
                sb.Append($"INSERT INTO \"{_options.Table}\" ({string.Join(", ", cols)}) SELECT {string.Join(", ", cols)} FROM \"{_stagingTable}\" ");
                
                if (_options.Strategy == PostgreSqlWriteStrategy.Ignore)
                {
                    sb.Append($"ON CONFLICT ({conflictTarget}) DO NOTHING");
                }
                else if (_options.Strategy == PostgreSqlWriteStrategy.Upsert)
                {
                     sb.Append($"ON CONFLICT ({conflictTarget}) DO UPDATE SET {updateSet}");
                }

                // Debug SQL
                Console.WriteLine($"[Postgres Merge SQL]: {sb}");

                await ExecuteNonQueryAsync(sb.ToString(), ct);
                // Temp table drops on commit/session end, but we can drop explicitly to be clean
                await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS \"{_stagingTable}\"", ct);
            }
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
            sb.Append($"\"{columns[i].Name}\" {PostgreSqlTypeMapper.Instance.MapToProviderType(columns[i].ClrType)}");
        }

        // PostgreSqlWriterOptions doesn't explicitly have Key property in current view, 
        // BUT I propagated it via reflection in JobService.
        // Wait, did I verify PostgreSqlWriterOptions HAS a Key property?
        // I checked file previously... let me verify if I added it?
        // Yes, in previous turn I viewed PostgreSqlWriterOptions and added Key.
        // So _options.Key should access it.
        // Wait, PostgreSqlDataWriter stores _options as PostgreSqlWriterOptions
        
        if (!string.IsNullOrEmpty(_options.Key))
        {
             // Use quoted column names for PK
             var keys = _options.Key.Split(',').Select(k => $"\"{k.Trim()}\"");
             sb.Append($", PRIMARY KEY ({string.Join(", ", keys)})");
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
