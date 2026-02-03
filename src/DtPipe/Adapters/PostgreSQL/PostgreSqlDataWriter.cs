using System.Text;
using Npgsql;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Helpers;

namespace DtPipe.Adapters.PostgreSQL;

public sealed partial class PostgreSqlDataWriter : IDataWriter, ISchemaInspector
{
    private readonly string _connectionString;
    private readonly PostgreSqlWriterOptions _options;
    private NpgsqlConnection? _connection;
    private NpgsqlBinaryImporter? _writer;
    private IReadOnlyList<ColumnInfo>? _columns;
    private string? _stagingTable;
    private string _quotedTargetTableName = ""; // Computed once after resolution, used everywhere
    private List<string> _keyColumns = new();
    
    private readonly ISqlDialect _dialect = new DtPipe.Core.Dialects.PostgreSqlDialect();
    public ISqlDialect Dialect => _dialect;

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

        // Resolve table using native logic
        var resolved = await ResolveTableAsync(connection, _options.Table, ct);
        
        if (resolved == null)
        {
             // Table does not exist (or at least could not be resolved)
             return new TargetSchemaInfo([], false, null, null, null);
        }

        var (schemaName, tableName) = resolved.Value;


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



    private static string BuildNativeType(string dataType, string udtName, int? maxLength, int? precision, int? scale)
    {
        return dataType.ToUpperInvariant() switch
        {
            "CHARACTER VARYING" when maxLength.HasValue => $"VARCHAR({maxLength})",
            "CHARACTER VARYING" => "VARCHAR",
            "CHARACTER" when maxLength.HasValue => $"CHAR({maxLength})",
            "CHARACTER" => "CHAR",
            "NUMERIC" when precision.HasValue && scale.HasValue => $"NUMERIC({precision},{scale})",
            "NUMERIC" when precision.HasValue => $"NUMERIC({precision})",
            "NUMERIC" => "NUMERIC",
            _ => udtName.ToUpperInvariant()
        };
    }

    #endregion

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        _connection = new NpgsqlConnection(_connectionString);
        await _connection.OpenAsync(ct);
        // GLOBAL NORMALIZATION:
        // Create a secure list of columns where names are normalized if not case-sensitive.
        // This ensures consistency across CREATE TABLE, INSERT, COPY, etc.
        var normalizedColumns = new List<ColumnInfo>(columns.Count);
        foreach (var col in columns)
        {
            if (col.IsCaseSensitive)
            {
                normalizedColumns.Add(col);
            }
            else
            {
                // Unquoted/Insensitive -> Normalize to PostgreSQL default (lowercase)
                // e.g. "UserName" -> "username"
                normalizedColumns.Add(col with { Name = _dialect.Normalize(col.Name) });
            }
        }
        _columns = normalizedColumns;

        string resolvedSchema;
        string resolvedTable;
        
        // 1. Native Resolution
        var resolved = await ResolveTableAsync(_connection, _options.Table, ct);

        if (resolved != null)
        {
            // Table exists (or at least resolved to an object)
            resolvedSchema = resolved.Value.Schema;
            resolvedTable = resolved.Value.Table;
        }
        else
        {
            // Table does not exist (or could not be resolved)
            if (_options.Strategy == PostgreSqlWriteStrategy.Recreate)
            {
                // Fallback: Parse user input to determine where to create the new table
                var (s, t) = ParseTableName(_options.Table);
                resolvedSchema = s;
                resolvedTable = t;
            }
            else
            {
                // For other strategies (Append, Truncate, etc.), if table doesn't exist,
                // we'll try to create it (preserving original behavior)
                var (s, t) = ParseTableName(_options.Table);
                resolvedSchema = s;
                resolvedTable = t;
            }
        }
        
        // Compute quoted table name ONCE for consistency across all SQL statements
        // Smart quoting: only quote schema/table if necessary (mixed case, special chars, reserved words)
        var safeSchema = _dialect.NeedsQuoting(resolvedSchema) ? _dialect.Quote(resolvedSchema) : resolvedSchema;
        var safeTable = _dialect.NeedsQuoting(resolvedTable) ? _dialect.Quote(resolvedTable) : resolvedTable;
        // If schema is empty, use table name alone (let PostgreSQL resolve via search_path)
        _quotedTargetTableName = string.IsNullOrEmpty(safeSchema) ? safeTable : $"{safeSchema}.{safeTable}";

        // Handle Strategy-Specific Logic
        if (_options.Strategy == PostgreSqlWriteStrategy.Recreate)
        {
            // Recreate: Always drop (if exists) and create fresh table
            if (resolved != null)
            {
                await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_quotedTargetTableName}", ct);
            }
            // Create table
            var createSql = BuildCreateTableSql(_quotedTargetTableName, _columns);
            await ExecuteNonQueryAsync(createSql, ct);
        }
        else if (_options.Strategy == PostgreSqlWriteStrategy.DeleteThenInsert)
        {
            // Table should exist - if not, create it
            if (resolved == null)
            {
                var createSql = BuildCreateTableSql(_quotedTargetTableName, _columns);
                await ExecuteNonQueryAsync( createSql, ct);
            }
            await ExecuteNonQueryAsync($"DELETE FROM {_quotedTargetTableName}", ct);
        }
        else if (_options.Strategy == PostgreSqlWriteStrategy.Truncate)
        {
            // Table should exist - if not, create it first
            if (resolved == null)
            {
                var createSql = BuildCreateTableSql(_quotedTargetTableName, _columns);
                await ExecuteNonQueryAsync(createSql, ct);
            }
            await ExecuteNonQueryAsync($"TRUNCATE TABLE {_quotedTargetTableName}", ct);
        }
        else // Append, Upsert, Ignore
        {
            // Create table only if it doesn't exist
            if (resolved == null)
            {
                var createSql = BuildCreateTableSql(_quotedTargetTableName, _columns);
                await ExecuteNonQueryAsync(createSql, ct);
            }
        }

        // Incremental Loading Setup

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
                 _keyColumns.AddRange(ColumnHelper.ResolveKeyColumns(_options.Key, _columns));
            }

            if (_keyColumns.Count == 0)
            {
                  throw new InvalidOperationException($"Strategy {_options.Strategy} requires a Primary Key. None detected. Please ensure the target table has a primary key.");
            }
             
            // 2. Create Staging Table (TEMP table)
            _stagingTable = $"tmp_stage_{Guid.NewGuid():N}"; // Safe GUID
            
            // Remove ON COMMIT DROP because in autocommit mode it drops immediately.
            // We explicit drop in CompleteAsync anyway.
             await ExecuteNonQueryAsync($"CREATE TEMP TABLE {_stagingTable} (LIKE {_quotedTargetTableName} INCLUDING DEFAULTS)", ct);
        }

        // Begin Binary Import
        // Construct COPY command - use staging table if Upsert/Ignore, otherwise use target
        var copyTarget = _stagingTable ?? _quotedTargetTableName;
        var copySql = BuildCopySql(copyTarget, _columns);
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
                var cols = _columns!.Select(c => SqlIdentifierHelper.GetSafeIdentifier(_dialect, c)).ToList();
                var rawCols = _columns!.Select(c => c.Name).ToList();

                var conflictTarget = string.Join(", ", _keyColumns.Select(c => _dialect.Quote(c)));
                
                // Exclude keys from update set
                var updateSet = string.Join(", ", 
                    _columns!.Where(c => !_keyColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase))
                             .Select(c => $"{SqlIdentifierHelper.GetSafeIdentifier(_dialect, c)} = EXCLUDED.{SqlIdentifierHelper.GetSafeIdentifier(_dialect, c)}"));

                var sb = new StringBuilder();
                string quotedStaging = _dialect.Quote(_stagingTable);

                sb.Append($"INSERT INTO {_quotedTargetTableName} ({string.Join(", ", cols)}) SELECT {string.Join(", ", cols)} FROM {quotedStaging} ");
                
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
                await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_stagingTable}", ct);
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

    /// <summary>
    /// Builds CREATE TABLE DDL from source column info.
    /// </summary>
    /// <remarks>
    /// NOTE: Types are mapped from CLR types (e.g., decimal → NUMERIC, string → TEXT),
    /// not preserved from target schema. Type precision, scale, and length constraints
    /// may differ from the original table when using the Recreate strategy.
    /// 
    /// For exact structure preservation, use Append strategy or manage DDL separately.
    /// </remarks>
    private string BuildCreateTableSql(string quotedTableName, IReadOnlyList<ColumnInfo> columns)
    {
        var sb = new StringBuilder();
        // Table name is already quoted if necessary by caller
        sb.Append($"CREATE TABLE IF NOT EXISTS {quotedTableName} (");
        
        for (int i = 0; i < columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            
            var col = columns[i];
            string nameToUse = col.Name;
            
            // Normalize if not case sensitive to avoid unnecessary quoting of case mismatches
            if (!col.IsCaseSensitive)
            {
                nameToUse = _dialect.Normalize(col.Name);
            }
            
            string safeName = SqlIdentifierHelper.GetSafeIdentifier(_dialect, nameToUse);
            sb.Append($"{safeName} {PostgreSqlTypeMapper.Instance.MapToProviderType(col.ClrType)}");
        }
        
        if (!string.IsNullOrEmpty(_options.Key))
        {
             var resolvedKeys = ColumnHelper.ResolveKeyColumns(_options.Key, columns.ToList());
             var safeKeys = resolvedKeys.Select(keyName =>
             {
                 var col = columns.First(c => c.Name == keyName);
                 return SqlIdentifierHelper.GetSafeIdentifier(_dialect, col);
             }).ToList();
             sb.Append($", PRIMARY KEY ({string.Join(", ", safeKeys)})");
        }
        
        sb.Append(")");

        return sb.ToString();
    }

    private string BuildCopySql(string tableName, IReadOnlyList<ColumnInfo> columns)
    {
        var sb = new StringBuilder();
        // IMPORTANT: tableName is already smart-quoted by caller (_quotedTargetTableName or _stagingTable)
        // Do NOT re-quote it or PostgreSQL will interpret "schema.table" as a single table name with a dot!
        sb.Append($"COPY {tableName} (");
        for (int i = 0; i < columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append(SqlIdentifierHelper.GetSafeIdentifier(_dialect, columns[i]));
        }
        sb.Append(") FROM STDIN (FORMAT BINARY)");
        return sb.ToString();
    }
    
    // Legacy helper 
    private string BuildCopySqlUnquoted(string tableName, IReadOnlyList<ColumnInfo> columns) => BuildCopySql(tableName, columns);

    private static async Task<(string Schema, string Table)?> ResolveTableAsync(NpgsqlConnection connection, string inputName, CancellationToken ct = default)
    {
        // Use to_regclass to resolve the table name using search_path
        // This handles "table", "schema.table", "quoted.table", etc.
        // It returns NULL if not found.
        
        var sql = @"
            SELECT n.nspname, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.oid = to_regclass(@input)::oid";

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("input", inputName);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return (reader.GetString(0), reader.GetString(1));
        }
        
        return null;
    }

    private (string Schema, string Table) ParseTableName(string tableName)
    {
        // Fallback manual parser for Creation scenarios where table doesn't exist yet
        // IMPORTANT: If no schema specified, return EMPTY schema to let PostgreSQL use search_path
        if (string.IsNullOrWhiteSpace(tableName)) return ("", tableName);
        
        string[] parts = tableName.Split('.');
        if (parts.Length == 2)
        {
             return (NormalizeIdentifier(parts[0]), NormalizeIdentifier(parts[1]));
        }
        
        // No schema specified - return empty schema, let PostgreSQL resolve via search_path
        return ("", NormalizeIdentifier(tableName));
    }
    
    private string NormalizeIdentifier(string id)
    {
        return id.Trim('"');
    }
}
