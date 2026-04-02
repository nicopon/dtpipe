using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Types;
using DtPipe.Adapters.Common;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Npgsql;

namespace DtPipe.Adapters.PostgreSQL;

/// <summary>
/// Columnar stream reader for PostgreSQL. Produces Apache Arrow RecordBatches directly
/// from NpgsqlDataReader via typed column consumers (no boxing).
/// Implements both IStreamReader (row-mode fallback) and IColumnarStreamReader (Arrow mode).
/// </summary>
public sealed partial class PostgreSqlColumnarReader : IColumnarStreamReader
{
    private readonly string _connectionString;
    private readonly string _query;
    private readonly int _timeout;
    private NpgsqlConnection? _connection;
    private NpgsqlCommand? _command;
    private NpgsqlDataReader? _reader;
    private AdoToArrowConfig? _config;
    private Func<IArrowType, int, IAdoConsumer>? _consumerFactory;

    public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
    public Schema? Schema { get; private set; }

    [GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
    private static partial Regex FirstWordRegex();

    private static readonly string[] DdlKeywords =
    {
        "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME",
        "GRANT", "REVOKE", "COMMENT", "VACUUM", "MIGRATE",
        "INSERT", "UPDATE", "DELETE", "MERGE", "CALL", "DO",
        "LOCK", "EXPLAIN", "ANALYZE"
    };

    public PostgreSqlColumnarReader(string connectionString, string query, int timeout = 0)
    {
        ValidateQueryIsSafeSelect(query);
        _connectionString = connectionString;
        _query = query;
        _timeout = timeout;
    }

    private static void ValidateQueryIsSafeSelect(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentException("Query cannot be empty.", nameof(query));

        var match = FirstWordRegex().Match(query);
        if (!match.Success)
            throw new ArgumentException("Invalid query format.", nameof(query));

        var firstWord = match.Groups[1].Value.ToUpperInvariant();
        if (firstWord != "SELECT" && firstWord != "WITH" && firstWord != "VALUES")
            throw new InvalidOperationException($"Only SELECT queries are allowed. Detected: {firstWord}");
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        _connection = new NpgsqlConnection(_connectionString);
        await _connection.OpenAsync(ct);

        _command = new NpgsqlCommand(_query, _connection);
        if (_timeout > 0) _command.CommandTimeout = _timeout;

        _reader = await _command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);

        var dbColumns = await _reader.GetColumnSchemaAsync(ct);

        // Build PipeColumnInfo from DB schema (CLR types) — authoritative for DtPipe pipeline
        Columns = dbColumns.Select(c => new PipeColumnInfo(
            c.ColumnName,
            c.DataType ?? typeof(object),
            c.AllowDBNull ?? true,
            IsCaseSensitive: c.ColumnName != c.ColumnName.ToLowerInvariant()
        )).ToList();

        // Build Arrow schema from PipeColumnInfo via ArrowTypeMapper — guarantees consistency
        Schema = ArrowSchemaFactory.Create(Columns);

        // Build consumer factory with DtPipe type semantics (handles Guid → BinaryType)
        var guidColumnIndexes = new HashSet<int>(
            dbColumns.Select((c, i) => (c, i))
                     .Where(x => x.c.DataType == typeof(Guid))
                     .Select(x => x.i));

        _config = new AdoToArrowConfigBuilder()
            .SetTypeResolver(col => ArrowTypeMapper.GetLogicalType(
                Nullable.GetUnderlyingType(col.DataType ?? typeof(string)) ?? col.DataType ?? typeof(string)))
            .Build();

        _consumerFactory = (arrowType, colIdx) =>
            guidColumnIndexes.Contains(colIdx)
                ? new GuidAsBytesConsumer(colIdx)
                : AdoConsumerFactory.Create(arrowType, colIdx);
    }

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_reader is null || Schema is null)
            throw new InvalidOperationException("Call OpenAsync first.");

        await foreach (var batch in AdoToArrow.ReadToArrowBatchesAsync(
            _reader, _config, _consumerFactory, ct))
        {
            yield return batch;
        }
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_reader is null) throw new InvalidOperationException("Call OpenAsync first.");

        var batch = new object?[batchSize][];
        var index = 0;

        while (await _reader.ReadAsync(ct))
        {
            var row = new object[_reader.FieldCount];
            _reader.GetValues(row);
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

    public async ValueTask DisposeAsync()
    {
        if (_reader != null) await _reader.DisposeAsync();
        if (_command != null) await _command.DisposeAsync();
        if (_connection != null) await _connection.DisposeAsync();
    }
}
