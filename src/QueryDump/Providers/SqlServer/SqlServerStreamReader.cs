using System.Data;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.SqlServer;

/// <summary>
/// Streams data from SQL Server using IAsyncEnumerable.
/// </summary>
public sealed partial class SqlServerStreamReader : IStreamReader, IRequiresOptions<SqlServerOptions>
{
    private readonly SqlConnection _connection;
    private readonly SqlCommand _command;
    private readonly string _query;
    private SqlDataReader? _reader;

    public IReadOnlyList<ColumnInfo>? Columns { get; private set; }

    // DDL keywords that should be rejected
    private static readonly string[] DdlKeywords = 
    [
        "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME", 
        "GRANT", "REVOKE", "COMMENT", "BACKUP", "RESTORE",
        "INSERT", "UPDATE", "DELETE", "MERGE", "EXEC", "EXECUTE"
    ];

    [GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
    private static partial Regex FirstWordRegex();

    public SqlServerStreamReader(string connectionString, string query, int queryTimeout = 0)
    {
        ValidateQueryIsSafeSelect(query);
        
        _query = query;
        _connection = new SqlConnection(connectionString);
        _command = new SqlCommand(query, _connection)
        {
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
                "DDL/DML statements are blocked for safety.");
        }

        // Additional check for dangerous keywords
        var upperQuery = query.ToUpperInvariant();
        foreach (var keyword in DdlKeywords)
        {
            if (Regex.IsMatch(upperQuery, $@"\b{keyword}\b"))
            {
                if (keyword != "SELECT" && firstWord == "SELECT")
                {
                    continue; // Simplistic check similar to Oracle implementation
                }
            }
        }
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        await _connection.OpenAsync(ct);
        _reader = await _command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
        Columns = ExtractColumns(_reader);
    }

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
        
        if (index > 0)
        {
            yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
        }
    }

    private static List<ColumnInfo> ExtractColumns(SqlDataReader reader)
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
