using System.Text;
using Microsoft.Data.Sqlite;
using QueryDump.Core.Abstractions;
using QueryDump.Cli.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Core.Options;
using QueryDump.Core.Helpers;

namespace QueryDump.Adapters.Sqlite;

public class SqliteDataWriter : IDataWriter, ISchemaInspector
{
    private readonly string _connectionString;
    private readonly OptionsRegistry _registry;

    private SqliteConnection? _connection;
    private IReadOnlyList<ColumnInfo>? _columns;
    private string _tableName = "Export";
    private SqliteWriteStrategy _strategy = SqliteWriteStrategy.Append;

    public long BytesWritten { get; private set; }

    public SqliteDataWriter(string connectionString, OptionsRegistry registry)
    {
        _connectionString = connectionString;
        _registry = registry;
    }

    #region ISchemaInspector Implementation

    public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        var options = _registry.Get<SqliteWriterOptions>();
        var tableName = options.Table;

        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);

        // Check if table exists
        using var existsCmd = connection.CreateCommand();
        existsCmd.CommandText = $"SELECT name FROM sqlite_master WHERE type='table' AND name='{tableName}'";
        var exists = await existsCmd.ExecuteScalarAsync(ct) != null;
        
        if (!exists)
        {
            return new TargetSchemaInfo([], false, null, null, null);
        }

        // Get columns using PRAGMA table_info
        using var columnsCmd = connection.CreateCommand();
        columnsCmd.CommandText = $"PRAGMA table_info(\"{tableName}\")";

        var pkColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var columns = new List<TargetColumnInfo>();
        
        using var reader = await columnsCmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            var colName = reader.GetString(1);
            var dataType = reader.GetString(2);
            var notNull = reader.GetInt32(3) == 1;
            var isPk = reader.GetInt32(5) > 0;

            if (isPk)
            {
                pkColumns.Add(colName);
            }

            columns.Add(new TargetColumnInfo(
                colName,
                dataType.ToUpperInvariant(),
                MapSqliteToClr(dataType),
                !notNull && !isPk, // Nullable if not marked NOT NULL and not PK
                isPk,
                false, // SQLite doesn't easily expose UNIQUE via PRAGMA
                ExtractMaxLength(dataType)
            ));
        }

        // Get row count
        long? rowCount = null;
        try
        {
            using var countCmd = connection.CreateCommand();
            countCmd.CommandText = $"SELECT COUNT(*) FROM \"{tableName}\"";
            var countResult = await countCmd.ExecuteScalarAsync(ct);
            rowCount = countResult == null ? null : Convert.ToInt64(countResult);
        }
        catch { /* Row count not available */ }

        // Get file size (database file size)
        long? sizeBytes = null;
        try
        {
            // Extract file path from connection string
            var builder = new SqliteConnectionStringBuilder(_connectionString);
            if (!string.IsNullOrEmpty(builder.DataSource) && File.Exists(builder.DataSource))
            {
                sizeBytes = new FileInfo(builder.DataSource).Length;
            }
        }
        catch { /* Size info not available */ }

        return new TargetSchemaInfo(
            columns,
            true,
            rowCount,
            sizeBytes,
            pkColumns.Count > 0 ? pkColumns.ToList() : null
        );
    }

    private static Type? MapSqliteToClr(string dataType)
    {
        // SQLite uses type affinity, so we need to parse the type name
        var upperType = dataType.ToUpperInvariant();
        
        if (upperType.Contains("INT")) return typeof(long);
        if (upperType.Contains("CHAR") || upperType.Contains("TEXT") || upperType.Contains("CLOB")) return typeof(string);
        if (upperType.Contains("BLOB")) return typeof(byte[]);
        if (upperType.Contains("REAL") || upperType.Contains("FLOA") || upperType.Contains("DOUB")) return typeof(double);
        if (upperType.Contains("BOOL")) return typeof(bool);
        if (upperType.Contains("DATE") || upperType.Contains("TIME")) return typeof(DateTime);
        if (upperType.Contains("DECIMAL") || upperType.Contains("NUMERIC")) return typeof(decimal);
        
        // Default to string for NUMERIC affinity or unknown types
        return typeof(string);
    }

    private static int? ExtractMaxLength(string dataType)
    {
        // Try to extract length from types like VARCHAR(100), CHAR(50), etc.
        var match = System.Text.RegularExpressions.Regex.Match(dataType, @"\((\d+)\)");
        if (match.Success && int.TryParse(match.Groups[1].Value, out var length))
        {
            return length;
        }
        return null;
    }

    #endregion


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

        if (_strategy == SqliteWriteStrategy.Recreate)
        {
            cmd.CommandText = $"DROP TABLE IF EXISTS \"{_tableName}\"";
            await cmd.ExecuteNonQueryAsync(ct);
            await CreateTableAsync(ct);
        }
        else if (_strategy == SqliteWriteStrategy.Truncate)
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

        try
        {
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
        catch (Exception ex)
        {
             var analysis = await BatchFailureAnalyzer.AnalyzeAsync(this, rows, _columns!, ct);
             if (!string.IsNullOrEmpty(analysis))
            {
                throw new InvalidOperationException($"SQLite Insert Failed with detailed analysis:\n{analysis}", ex);
            }
            throw;
        }
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
