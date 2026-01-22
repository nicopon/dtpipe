using Microsoft.Data.Sqlite;
using QueryDump.Core;

namespace QueryDump.Adapters.Sqlite;

public class SqliteStreamReader : IStreamReader
{
    private readonly string _connectionString;
    private readonly string _query;
    private readonly int _queryTimeout;
    
    private SqliteConnection? _connection;
    private SqliteCommand? _command;
    private SqliteDataReader? _reader;

    public IReadOnlyList<ColumnInfo>? Columns { get; private set; }

    public SqliteStreamReader(string connectionString, string query, int queryTimeout = 0)
    {
        _connectionString = connectionString;
        _query = query;
        _queryTimeout = queryTimeout;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        _connection = new SqliteConnection(_connectionString);
        await _connection.OpenAsync(ct);

        _command = _connection.CreateCommand();
        _command.CommandText = _query;
        if (_queryTimeout > 0)
        {
            _command.CommandTimeout = _queryTimeout;
        }

        _reader = await _command.ExecuteReaderAsync(ct);
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

    private static List<ColumnInfo> ExtractColumns(SqliteDataReader reader)
    {
        var columns = new List<ColumnInfo>(reader.FieldCount);
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columns.Add(new ColumnInfo(
                reader.GetName(i),
                reader.GetFieldType(i),
                true)); // SQLite is dynamically typed, assume nullable
        }
        return columns;
    }

    public async ValueTask DisposeAsync()
    {
        if (_reader != null)
        {
            await _reader.DisposeAsync();
            _reader = null;
        }
        if (_command != null)
        {
            await _command.DisposeAsync();
            _command = null;
        }
        if (_connection != null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }
}
