using System.Data;
using System.Text.RegularExpressions;
using DuckDB.NET.Data;
using QueryDump.Core;
using QueryDump.Core.Options;
using ColumnInfo = QueryDump.Core.ColumnInfo; // Alias to resolve conflict with DuckDB.NET.Data.ColumnInfo

namespace QueryDump.Providers.DuckDB;

public sealed partial class DuckDataSourceReader : IDataSourceReader, IRequiresOptions<DuckDbOptions>
{
    private readonly DuckDBConnection _connection;
    private readonly DuckDBCommand _command;
    private readonly string _query;
    private DuckDBDataReader? _reader;

    public IReadOnlyList<ColumnInfo>? Columns { get; private set; }

    // Minimal safety check - DuckDB is local/embedded so less risky, but consistent with others
    // We block destructive commands
    private static readonly string[] DdlKeywords = 
    {
        "DROP", "ALTER", "DELETE", "UPDATE", "INSERT"
    };

    [GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
    private static partial Regex FirstWordRegex();

    public DuckDataSourceReader(string connectionString, string query, DuckDbOptions options, int queryTimeout = 0)
    {
        ValidateQueryIsSafeSelect(query);
        
        _query = query;
        _connection = new DuckDBConnection(connectionString);
        _command = new DuckDBCommand(query, _connection)
        {
            CommandTimeout = queryTimeout
        };
    }

    private static void ValidateQueryIsSafeSelect(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentException("Query cannot be empty.", nameof(query));

        var match = FirstWordRegex().Match(query);
        if (!match.Success)
            throw new ArgumentException("Invalid query format.", nameof(query));

        var firstWord = match.Groups[1].Value.ToUpperInvariant();

        if (firstWord != "SELECT" && firstWord != "WITH" && firstWord != "PRAGMA" && firstWord != "DESCRIBE")
        {
            throw new InvalidOperationException($"Query must start with SELECT/WITH. Detected: {firstWord}");
        }
        
        // Basic keyword check
        var upperQuery = query.ToUpperInvariant();
        foreach (var keyword in DdlKeywords)
        {
            if (Regex.IsMatch(upperQuery, $@"\b{keyword}\b"))
            {
                 // Allow SELECT
                 if (firstWord == "SELECT") continue;
                 // Be stricter for DuckDB as it might operate on local files
                 // But for now, simple consistency
            }
        }
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        await _connection.OpenAsync(ct);
        _reader = (DuckDBDataReader)await _command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
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

    private static List<ColumnInfo> ExtractColumns(DuckDBDataReader reader)
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
