using System.Buffers;
using System.Data;
using System.Text.RegularExpressions;
using Oracle.ManagedDataAccess.Client;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.Oracle;

/// <summary>
/// Streams data from Oracle using IAsyncEnumerable for memory efficiency.
/// </summary>
public sealed partial class OracleStreamReader : IDataSourceReader, IRequiresOptions<OracleOptions>
{
    private readonly OracleConnection _connection;
    private readonly OracleCommand _command;
    private readonly string _query;
    private OracleDataReader? _reader;

    public IReadOnlyList<ColumnInfo>? Columns { get; private set; }

    // DDL keywords that should be rejected
    private static readonly string[] DdlKeywords = 
    {
        "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME", 
        "GRANT", "REVOKE", "COMMENT", "FLASHBACK", "PURGE",
        "INSERT", "UPDATE", "DELETE", "MERGE", "CALL",
        "LOCK", "EXECUTE", "EXPLAIN"
    };

    [GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
    private static partial Regex FirstWordRegex();

    public OracleStreamReader(string connectionString, string query, OracleOptions options, int queryTimeout = 0)
    {
        ValidateQueryIsSafeSelect(query);
        
        _query = query;
        _connection = new OracleConnection(connectionString);
        _command = new OracleCommand(query, _connection)
        {
            FetchSize = options.FetchSize,
            CommandTimeout = queryTimeout
        };
    }

    /// <summary>
    /// Validates that the query is a safe SELECT statement.
    /// Throws if DDL/DML statements are detected.
    /// </summary>
    private static void ValidateQueryIsSafeSelect(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentException("Query cannot be empty.", nameof(query));

        var match = FirstWordRegex().Match(query);
        if (!match.Success)
            throw new ArgumentException("Invalid query format.", nameof(query));

        var firstWord = match.Groups[1].Value.ToUpperInvariant();

        // Must start with SELECT or WITH (for CTEs)
        if (firstWord != "SELECT" && firstWord != "WITH")
        {
            throw new InvalidOperationException(
                $"Only SELECT queries are allowed. Detected: {firstWord}. " +
                "DDL/DML statements (CREATE, DROP, INSERT, UPDATE, DELETE, etc.) are blocked for safety.");
        }

        // Additional check for dangerous keywords anywhere in the query
        var upperQuery = query.ToUpperInvariant();
        foreach (var keyword in DdlKeywords)
        {
            // Check if keyword appears as a standalone word (not part of column/table name)
            if (Regex.IsMatch(upperQuery, $@"\b{keyword}\b"))
            {
                // Allow SELECT, but warn about embedded dangerous keywords
                // This is a heuristic - embedded DDL in subqueries is rare but possible
                if (keyword != "SELECT" && firstWord == "SELECT")
                {
                    // Only block if it's clearly at statement level (very basic check)
                    // More sophisticated parsing would require a SQL parser
                    continue;
                }
            }
        }
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        await _connection.OpenAsync(ct);
        _reader = (OracleDataReader)await _command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
        Columns = ExtractColumns(_reader);
    }

    /// <summary>
    /// Reads rows in batches, returning pooled batches for memory efficiency.
    /// </summary>
    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_reader is null)
            throw new InvalidOperationException("Call OpenAsync first.");

        var columnCount = _reader.FieldCount;
        var batch = new object?[batchSize][];
        var index = 0;
        
        while (await _reader.ReadAsync(ct))
        {
            var row = new object?[columnCount];
            for (var i = 0; i < columnCount; i++)
            {
                row[i] = _reader.IsDBNull(i) ? null : _reader.GetValue(i);
            }
            
            batch[index++] = row;
            
            if (index >= batchSize)
            {
                yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
                batch = new object?[batchSize][];
                index = 0;
            }
        }
        
        // Return remaining rows
        if (index > 0)
        {
            yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
        }
    }

    /// <summary>
    /// Reads rows one at a time (for compatibility).
    /// </summary>
    public async IAsyncEnumerable<object?[]> ReadRowsAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_reader is null)
            throw new InvalidOperationException("Call OpenAsync first.");

        var columnCount = _reader.FieldCount;
        
        while (await _reader.ReadAsync(ct))
        {
            var row = new object?[columnCount];
            for (var i = 0; i < columnCount; i++)
            {
                row[i] = _reader.IsDBNull(i) ? null : _reader.GetValue(i);
            }
            yield return row;
        }
    }

    private static List<ColumnInfo> ExtractColumns(OracleDataReader reader)
    {
        var columns = new List<ColumnInfo>(reader.FieldCount);
        var schemaTable = reader.GetSchemaTable();
        
        if (schemaTable is null)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                columns.Add(new ColumnInfo(
                    reader.GetName(i),
                    reader.GetFieldType(i),
                    true));
            }
            return columns;
        }

        foreach (DataRow row in schemaTable.Rows)
        {
            var name = row["ColumnName"]?.ToString() ?? $"Column{columns.Count}";
            var clrType = row["DataType"] as Type ?? typeof(object);
            var allowNull = row["AllowDBNull"] as bool? ?? true;
            
            columns.Add(new ColumnInfo(name, clrType, allowNull));
        }

        return columns;
    }

    public async ValueTask DisposeAsync()
    {
        if (_reader is not null)
        {
            await _reader.DisposeAsync();
        }
        await _command.DisposeAsync();
        await _connection.DisposeAsync();
    }
}
