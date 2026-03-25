using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Microsoft.Data.Sqlite;

namespace DtPipe.Adapters.Sqlite;

/// <summary>
/// Columnar stream reader for SQLite. Produces Apache Arrow RecordBatches directly
/// from SqliteDataReader via typed column consumers (no boxing).
/// Implements both IStreamReader (row-mode fallback) and IColumnarStreamReader (Arrow mode).
/// </summary>
public sealed partial class SqliteColumnarReader : IColumnarStreamReader
{
    private readonly string _connectionString;
    private readonly string _query;
    private readonly int _queryTimeout;
    private SqliteConnection? _connection;
    private SqliteCommand? _command;
    private SqliteDataReader? _reader;
    private AdoToArrowConfig? _config;
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
    public Schema? Schema { get; private set; }

    [GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
    private static partial Regex FirstWordRegex();

    private static readonly string[] DdlKeywords =
    {
        "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME",
        "GRANT", "REVOKE", "VACUUM", "ATTACH", "DETACH",
        "INSERT", "UPDATE", "DELETE", "REPLACE"
    };

    public SqliteColumnarReader(string connectionString, string query, int queryTimeout = 0)
    {
        ValidateQueryIsSafeSelect(query);
        _connectionString = connectionString;
        _query = query;
        _queryTimeout = queryTimeout;
    }

    private static void ValidateQueryIsSafeSelect(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentException("Query cannot be empty.", nameof(query));

        var match = FirstWordRegex().Match(query);
        if (!match.Success)
            throw new ArgumentException("Invalid query format.", nameof(query));

        var firstWord = match.Groups[1].Value.ToUpperInvariant();
        if (firstWord != "SELECT" && firstWord != "WITH" && firstWord != "VALUES" && firstWord != "PRAGMA")
            throw new InvalidOperationException($"Only SELECT/PRAGMA queries are allowed. Detected: {firstWord}");
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        _connection = new SqliteConnection(_connectionString);
        await _connection.OpenAsync(ct);

        _command = _connection.CreateCommand();
        _command.CommandText = _query;
        if (_queryTimeout > 0) _command.CommandTimeout = _queryTimeout;

        _reader = await _command.ExecuteReaderAsync(ct);

        // SQLite is dynamically typed — use GetFieldType(i) directly, assume all nullable
        var columns = new List<PipeColumnInfo>(_reader.FieldCount);
        for (int i = 0; i < _reader.FieldCount; i++)
        {
            columns.Add(new PipeColumnInfo(
                _reader.GetName(i),
                _reader.GetFieldType(i),
                true,
                IsCaseSensitive: false));
        }
        Columns = columns;

        // Build Arrow schema from PipeColumnInfo via ArrowTypeMapper — guarantees consistency
        Schema = ArrowSchemaFactory.Create(Columns);

        // SQLite CLR types: long, double, string, byte[] — no special consumers needed
        _config = new AdoToArrowConfigBuilder()
            .SetTypeResolver(col => ArrowTypeMapper.GetArrowType(
                Nullable.GetUnderlyingType(col.DataType ?? typeof(string)) ?? col.DataType ?? typeof(string)))
            .Build();
    }

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_reader is null || Schema is null)
            throw new InvalidOperationException("Call OpenAsync first.");

        await _semaphore.WaitAsync(ct);
        try
        {
            await foreach (var batch in AdoToArrow.ReadToArrowBatchesAsync(_reader, _config, cancellationToken: ct))
            {
                yield return batch;
            }
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_reader is null) throw new InvalidOperationException("Call OpenAsync first.");

        await _semaphore.WaitAsync(ct);
        try
        {
            var columnCount = _reader.FieldCount;
            var batch = new object?[batchSize][];
            var index = 0;

            while (await _reader.ReadAsync(ct))
            {
                var row = new object?[columnCount];
                for (var i = 0; i < columnCount; i++)
                    row[i] = _reader.IsDBNull(i) ? null : _reader.GetValue(i);

                batch[index++] = row;

                if (index >= batchSize)
                {
                    yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
                    batch = new object?[batchSize][];
                    index = 0;
                }
            }

            if (index > 0)
                yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _semaphore.WaitAsync();
        try
        {
            if (_reader != null) { await _reader.DisposeAsync(); _reader = null; }
            if (_command != null) { await _command.DisposeAsync(); _command = null; }
            if (_connection != null) { await _connection.DisposeAsync(); _connection = null; }
        }
        finally
        {
            _semaphore.Release();
            _semaphore.Dispose();
        }
    }
}
