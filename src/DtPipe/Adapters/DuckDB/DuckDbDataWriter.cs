using DuckDB.NET.Data;
using DtPipe.Core.Abstractions;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using System.Text;
using DtPipe.Core.Helpers;
using ColumnInfo = DtPipe.Core.Models.ColumnInfo;

namespace DtPipe.Adapters.DuckDB;

public sealed class DuckDbDataWriter : IDataWriter, ISchemaInspector
{
    private readonly string _connectionString;
    private readonly DuckDBConnection _connection;
    private readonly DuckDbWriterOptions _options;
    private IReadOnlyList<ColumnInfo>? _columns;
    private string? _stagingTable; // Table to load data into before merging
    private List<string> _keyColumns = new();

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

        // Use PRAGMA table_info which gives Name, Type, NotNull, PK (boolean)
        using var columnsCmd = connection.CreateCommand();
        columnsCmd.CommandText = $"PRAGMA table_info('{tableName}')";

        long? rowCount = null;
        try
        {
            using var countCmd = connection.CreateCommand();
            countCmd.CommandText = $"SELECT COUNT(*) FROM {tableName}";
            var countResult = await countCmd.ExecuteScalarAsync(ct);
            rowCount = countResult == null ? null : Convert.ToInt64(countResult);
        }
        catch { /* Row count not available */ }

        var pkColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var columns = new List<TargetColumnInfo>();

        using var reader = await columnsCmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            // PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
            var colName = reader.GetString(1);
            var dataType = reader.GetString(2);
            var notNull = reader.GetBoolean(3);
            var isPk = reader.GetBoolean(5);

            if (isPk) pkColumns.Add(colName);
            
            columns.Add(new TargetColumnInfo(
                colName,
                dataType.ToUpperInvariant(),
                DuckDbTypeMapper.MapFromProviderType(dataType),
                !notNull,
                isPk,
                false, // Unique not easily available
                null
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
        else if (_options.Strategy == DuckDbWriteStrategy.DeleteThenInsert)
        {
             // Check if table exists before deleting
            var checkCmd = _connection.CreateCommand();
            checkCmd.CommandText = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{_options.Table}'";
            var exists = Convert.ToInt32(await checkCmd.ExecuteScalarAsync(ct)) > 0;
            
            if (exists)
            {
                var delCmd = _connection.CreateCommand();
                delCmd.CommandText = $"DELETE FROM {_options.Table}";
                await delCmd.ExecuteNonQueryAsync(ct);
            }
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
                truncCmd.CommandText = $"TRUNCATE TABLE {_options.Table}";
                await truncCmd.ExecuteNonQueryAsync(ct);
            }
        }

        var createTableSql = BuildCreateTableSql(_options.Table, columns);
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = createTableSql;
        await cmd.ExecuteNonQueryAsync(ct);

        // Incremental Loading Setup
        if (_options.Strategy == DuckDbWriteStrategy.Upsert || _options.Strategy == DuckDbWriteStrategy.Ignore)
        {
            // 1. Resolve Keys
            TargetSchemaInfo? targetInfoKeys = await InspectTargetAsync(ct);
            // Use manual keys if provided
            
            if (targetInfoKeys?.PrimaryKeyColumns != null)
            {
                _keyColumns.AddRange(targetInfoKeys.PrimaryKeyColumns);
            }

            // TODO: Fallback to manual Keys if passed?
            // The Architecture passes `options` (DuckDbWriterOptions) to Constructor.
            // I need to add `Key` to `DuckDbWriterOptions` or update `CliDataWriterFactory` to map it.
            
            if (_keyColumns.Count == 0 && string.IsNullOrEmpty(_options.Key))
            {
                 throw new InvalidOperationException($"Strategy {_options.Strategy} requires a Primary Key. None detected and none specified.");
            }

             // If manual key is in options
             if (!string.IsNullOrEmpty(_options.Key))
             {
                 _keyColumns.Clear();
                 _keyColumns.AddRange(_options.Key.Split(',').Select(k => k.Trim()));
             }

            // 2. Create Staging Table
            _stagingTable = $"{_options.Table}_stage_{Guid.NewGuid():N}";
            // Create stage with same schema as target (or source cols?)
            // Stage should match target schema to facilitate INSERT INTO SELECT
            var createStageSql = BuildPropertiesCopySql(_options.Table, _stagingTable); // DuckDB "CREATE TABLE stage AS SELECT * FROM target WHERE 0=1"
            
            using var stageCmd = _connection.CreateCommand();
            stageCmd.CommandText = $"CREATE TABLE {_stagingTable} AS SELECT * FROM {_options.Table} WHERE 1=0";
            await stageCmd.ExecuteNonQueryAsync(ct);
        }

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

        // Initialize Appender
        // If Staging, append to stage. Else append to target.
        var targetForAppender = _stagingTable ?? _options.Table;
        _appender = _connection.CreateAppender(targetForAppender);
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
        // Handle date/time types specifically for DuckDB compatibility
        else if (underlying == typeof(DateTime)) row.AppendValue((DateTime)val);
        else if (underlying == typeof(DateTimeOffset)) row.AppendValue((DateTimeOffset)val);
        else if (underlying == typeof(Guid)) row.AppendValue((Guid)val);
        else if (underlying == typeof(byte[])) row.AppendValue((byte[])val);
        else row.AppendValue(val.ToString());
    }

    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        _appender?.Dispose();
        _appender = null;

        if (_stagingTable != null)
        {
            try
            {
                // Perform Merge
                var cols = _columns!.Select(c => c.Name).ToList(); // Source columns
                // Actually we need the columns present in the table.
                
                // Construct ON CONFLICT clause
                var conflictTarget = string.Join(", ", _keyColumns);
                var updateSet = string.Join(", ", cols.Where(c => !_keyColumns.Contains(c, StringComparer.OrdinalIgnoreCase))
                                                      .Select(c => $"{c} = EXCLUDED.{c}"));

                var sql = new StringBuilder();
                sql.Append($"INSERT INTO {_options.Table} SELECT * FROM {_stagingTable} "); // Assumes generic matching
                
                if (_options.Strategy == DuckDbWriteStrategy.Ignore)
                {
                    sql.Append($"ON CONFLICT ({conflictTarget}) DO NOTHING");
                }
                else if (_options.Strategy == DuckDbWriteStrategy.Upsert)
                {
                    sql.Append($"ON CONFLICT ({conflictTarget}) DO UPDATE SET {updateSet}");
                }

                using var mergeCmd = _connection.CreateCommand();
                mergeCmd.CommandText = sql.ToString();
                await mergeCmd.ExecuteNonQueryAsync(ct);
            }
            finally
            {
                // Drop Stage
                using var dropCmd = _connection.CreateCommand();
                dropCmd.CommandText = $"DROP TABLE IF EXISTS {_stagingTable}";
                await dropCmd.ExecuteNonQueryAsync(ct);
            }
        }

        return; // Already completed task
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
            sb.Append($"{columns[i].Name} {DuckDbTypeMapper.MapToProviderType(columns[i].ClrType)}");
        }

        if (!string.IsNullOrEmpty(_options.Key))
        {
             sb.Append($", PRIMARY KEY ({_options.Key})");
        }
        
        sb.Append(")");
        return sb.ToString();
    }

    private string BuildPropertiesCopySql(string sourceTable, string targetTable)
    {
        // "CREATE TABLE target AS SELECT * FROM source WHERE 1=0"
        return $"CREATE TABLE {targetTable} AS SELECT * FROM {sourceTable} WHERE 1=0";
    }
}
