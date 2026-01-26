using DuckDB.NET.Data;
using QueryDump.Core.Abstractions;
using QueryDump.Cli.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Core.Options;
using System.Text;
using QueryDump.Core.Helpers;
using ColumnInfo = QueryDump.Core.Models.ColumnInfo;

namespace QueryDump.Adapters.DuckDB;

public sealed class DuckDbDataWriter : IDataWriter, ISchemaInspector
{
    private readonly string _connectionString;
    private readonly DuckDBConnection _connection;
    private readonly DuckDbWriterOptions _options;
    private IReadOnlyList<ColumnInfo>? _columns;

    public long BytesWritten => 0; 

    public DuckDbDataWriter(string connectionString, DuckDbWriterOptions options)
    {
        _connectionString = connectionString;
        _options = options;
        _connection = new DuckDBConnection(connectionString);
    }

    #region ISchemaInspector Implementation

    public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        await using var connection = new DuckDBConnection(_connectionString);
        await connection.OpenAsync(ct);

        var tableName = _options.Table;

        // Check if table exists
        using var existsCmd = connection.CreateCommand();
        existsCmd.CommandText = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{tableName}'";
        var exists = Convert.ToInt32(await existsCmd.ExecuteScalarAsync(ct)) > 0;
        
        if (!exists)
        {
            return new TargetSchemaInfo([], false, null, null, null);
        }

        // Get columns from information_schema
        var columnsSql = $@"
            SELECT 
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns 
            WHERE table_name = '{tableName}'
            ORDER BY ordinal_position";
        
        using var columnsCmd = connection.CreateCommand();
        columnsCmd.CommandText = columnsSql;

        // Get row count
        long? rowCount = null;
        try
        {
            using var countCmd = connection.CreateCommand();
            countCmd.CommandText = $"SELECT COUNT(*) FROM {tableName}";
            var countResult = await countCmd.ExecuteScalarAsync(ct);
            rowCount = countResult == null ? null : Convert.ToInt64(countResult);
        }
        catch { /* Row count not available */ }

        // DuckDB doesn't have easy access to PK/UNIQUE constraints via SQL
        // We'll return empty sets for now
        var pkColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var uniqueColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Build column list
        var columns = new List<TargetColumnInfo>();
        using var reader = await columnsCmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var colName = reader.GetString(0);
            var dataType = reader.GetString(1);
            var isNullable = reader.GetString(2) == "YES";

            columns.Add(new TargetColumnInfo(
                colName,
                dataType.ToUpperInvariant(),
                MapDuckDbToClr(dataType),
                isNullable,
                pkColumns.Contains(colName),
                uniqueColumns.Contains(colName),
                null // DuckDB doesn't expose max length easily
            ));
        }

        return new TargetSchemaInfo(
            columns,
            true,
            rowCount,
            null, // DuckDB doesn't expose table size easily
            pkColumns.Count > 0 ? pkColumns.ToList() : null
        );
    }

    private static Type? MapDuckDbToClr(string dataType)
    {
        // Handle parameterized types like DECIMAL(10,2) or VARCHAR(50)
        var baseType = dataType.Split('(')[0].Trim().ToUpperInvariant();

        return baseType switch
        {
            "TINYINT" => typeof(byte),
            "SMALLINT" => typeof(short),
            "INTEGER" or "INT" => typeof(int),
            "BIGINT" => typeof(long),
            "HUGEINT" => typeof(decimal),
            "FLOAT" or "REAL" => typeof(float),
            "DOUBLE" => typeof(double),
            "DECIMAL" or "NUMERIC" => typeof(decimal),
            "BOOLEAN" => typeof(bool),
            "VARCHAR" or "TEXT" or "STRING" => typeof(string),
            "DATE" => typeof(DateTime),
            "TIME" => typeof(TimeSpan),
            "TIMESTAMP" or "DATETIME" => typeof(DateTime),
            "TIMESTAMPTZ" => typeof(DateTimeOffset),
            "UUID" => typeof(Guid),
            "BLOB" or "BYTEA" => typeof(byte[]),
            _ => typeof(string)
        };
    }

    #endregion


    private IDisposable? _appender;
    private int[]? _columnMapping; // Maps: TargetIndex -> SourceIndex (or -1 if missing)

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
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
            // Check if table exists before truncating
            var checkCmd = _connection.CreateCommand();
            checkCmd.CommandText = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{_options.Table}'";
            var exists = Convert.ToInt32(await checkCmd.ExecuteScalarAsync(ct)) > 0;
            
            if (exists)
            {
                var truncCmd = _connection.CreateCommand();
                truncCmd.CommandText = $"DELETE FROM {_options.Table}";
                await truncCmd.ExecuteNonQueryAsync(ct);
            }
        }

        var createTableSql = BuildCreateTableSql(_options.Table, columns);
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = createTableSql;
        await cmd.ExecuteNonQueryAsync(ct);

        // Compute mapping: The Appender expects values in the PHYSICAL order of the table.
        // We must map that to our Source Columns.
        
        // 1. Get Target Columns in ordinal order
        var targetInfo = await InspectTargetAsync(ct);
        if (targetInfo != null && targetInfo.Columns.Count > 0)
        {
             _columnMapping = new int[targetInfo.Columns.Count];
             for(int i=0; i<targetInfo.Columns.Count; i++)
             {
                 var targetName = targetInfo.Columns[i].Name;
                 var sourceIndex = -1;
                 
                 // Find corresponding source column
                 for(int s=0; s<_columns.Count; s++)
                 {
                     if (string.Equals(_columns[s].Name, targetName, StringComparison.OrdinalIgnoreCase))
                     {
                         sourceIndex = s;
                         break;
                     }
                 }
                 _columnMapping[i] = sourceIndex;
             }
        }
        else
        {
            // Should not happen if table exists, fallback to 1:1
             _columnMapping = Enumerable.Range(0, _columns.Count).ToArray();
        }

        _appender = _connection.CreateAppender(_options.Table);
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_columns is null || _appender is null || _columnMapping is null) throw new InvalidOperationException("Not initialized");

        try
        {
            await Task.Run(() =>
            {
                var appender = (DuckDBAppender)_appender;
                foreach (var rowData in rows)
                {
                    var row = appender.CreateRow();
                    
                    // We iterate over the MAPPING (which corresponds to Target Table Columns in implicit order)
                    // for k = 0 (First column of table), we get matching Source Index.
                    for (int i = 0; i < _columnMapping.Length; i++)
                    {
                        var sourceIndex = _columnMapping[i];

                        if (sourceIndex == -1)
                        {
                            // Target table has a column that Source doesn't have.
                            // Append NULL.
                            row.AppendNullValue();
                        }
                        else
                        {
                            var val = rowData[sourceIndex];
                            var col = _columns[sourceIndex];
                            
                            if (val is null)
                            {
                                row.AppendNullValue();
                            }
                            else
                            {
                                AppendValue(row, val, col.ClrType);
                            }
                        }
                    }
                    row.EndRow();
                }
            }, ct);
        }
        catch (Exception ex)
        {
            var analysis = await BatchFailureAnalyzer.AnalyzeAsync(this, rows, _columns, ct);
             if (!string.IsNullOrEmpty(analysis))
            {
                throw new InvalidOperationException($"DuckDB Appender Failed with detailed analysis:\n{analysis}", ex);
            }
            throw;
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
        // Date/Time handling: DuckDB sometimes picky.
        else if (underlying == typeof(DateTime)) row.AppendValue((DateTime)val);
        else if (underlying == typeof(DateTimeOffset)) row.AppendValue((DateTimeOffset)val);
        else if (underlying == typeof(Guid)) row.AppendValue((Guid)val);
        else if (underlying == typeof(byte[])) row.AppendValue((byte[])val);
        else row.AppendValue(val.ToString());
    }

    public ValueTask CompleteAsync(CancellationToken ct = default)
    {
        _appender?.Dispose();
        _appender = null;
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _appender?.Dispose();
        await _connection.DisposeAsync();
    }

    private string BuildCreateTableSql(string tableName, IReadOnlyList<ColumnInfo> columns)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE IF NOT EXISTS {tableName} (");
        
        for (int i = 0; i < columns.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"{columns[i].Name} {DuckDbTypeMapper.MapClrType(columns[i].ClrType)}");
        }
        
        sb.Append(")");
        return sb.ToString();
    }
}
