using System.Data;
using System.Runtime.CompilerServices;
using Npgsql;
using QueryDump.Core;

namespace QueryDump.Providers.PostgreSQL;

public class PostgreSqlReader : IStreamReader
{
    private readonly string _connectionString;
    private readonly string _query;
    private readonly int _timeout;
    private NpgsqlConnection? _connection;
    private NpgsqlCommand? _command;
    private NpgsqlDataReader? _reader;
    public IReadOnlyList<ColumnInfo>? Columns { get; private set; }

    public PostgreSqlReader(string connectionString, string query, int timeout)
    {
        _connectionString = connectionString;
        _query = query;
        _timeout = timeout;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        _connection = new NpgsqlConnection(_connectionString);
        await _connection.OpenAsync(ct);

        _command = new NpgsqlCommand(_query, _connection);
        if (_timeout > 0)
        {
            _command.CommandTimeout = _timeout;
        }

        // Use SequentialAccess for performance
        _reader = await _command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
        
        // Populate Columns
        var schema = await _reader.GetColumnSchemaAsync(ct);
        // ColumnInfo(name, type, isNullable, isPrimaryKey)
        Columns = schema.Select(c => new ColumnInfo(c.ColumnName, c.DataType ?? typeof(object), c.AllowDBNull ?? true, c.IsKey ?? false)).ToList();
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_reader == null) throw new InvalidOperationException("Reader not opened");

        var batch = new List<object?[]>(batchSize);

        while (await _reader.ReadAsync(ct))
        {
            var row = new object?[_reader.FieldCount];
            _reader.GetValues((object[])row);
            batch.Add(row);

            if (batch.Count >= batchSize)
            {
                yield return batch.ToArray();
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            yield return batch.ToArray();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_reader != null) await _reader.DisposeAsync();
        if (_command != null) await _command.DisposeAsync();
        if (_connection != null) await _connection.DisposeAsync();
    }
}
