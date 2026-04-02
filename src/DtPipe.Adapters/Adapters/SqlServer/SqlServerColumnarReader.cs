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
using DtPipe.Core.Options;
using Microsoft.Data.SqlClient;

namespace DtPipe.Adapters.SqlServer;

/// <summary>
/// Columnar stream reader for SQL Server. Produces Apache Arrow RecordBatches directly
/// from SqlDataReader via typed column consumers (no boxing).
/// Implements both IStreamReader (row-mode fallback) and IColumnarStreamReader (Arrow mode).
/// </summary>
public sealed partial class SqlServerColumnarReader : IColumnarStreamReader, IRequiresOptions<SqlServerReaderOptions>
{
    private readonly SqlConnection _connection;
    private readonly SqlCommand _command;
    private SqlDataReader? _reader;
    private AdoToArrowConfig? _config;
    private Func<IArrowType, int, IAdoConsumer>? _consumerFactory;

    public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
    public Schema? Schema { get; private set; }

    [GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
    private static partial Regex FirstWordRegex();

    private static readonly string[] DdlKeywords =
    [
        "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME",
        "GRANT", "REVOKE", "COMMENT", "BACKUP", "RESTORE",
        "INSERT", "UPDATE", "DELETE", "MERGE", "EXEC", "EXECUTE"
    ];

    public SqlServerColumnarReader(string connectionString, string query, SqlServerReaderOptions options, int queryTimeout = 0)
    {
        ValidateQueryIsSafeSelect(query);
        _connection = new SqlConnection(connectionString);
        _command = new SqlCommand(query, _connection) { CommandTimeout = queryTimeout };
    }

    private static void ValidateQueryIsSafeSelect(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentException("Query cannot be empty.", nameof(query));

        var match = FirstWordRegex().Match(query);
        if (!match.Success)
            throw new ArgumentException("Invalid query format.", nameof(query));

        var firstWord = match.Groups[1].Value.ToUpperInvariant();
        if (firstWord != "SELECT" && firstWord != "WITH")
            throw new InvalidOperationException(
                $"Only SELECT queries are allowed. Detected: {firstWord}. DDL/DML statements are blocked for safety.");
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        await _connection.OpenAsync(ct);
        _reader = await _command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);

        var dbColumns = _reader.GetColumnSchema();

        // Build PipeColumnInfo from DB schema (CLR types) — authoritative for DtPipe pipeline
        var columns = new List<PipeColumnInfo>(dbColumns.Count);
        foreach (var col in dbColumns)
        {
            columns.Add(new PipeColumnInfo(
                col.ColumnName,
                col.DataType ?? typeof(object),
                col.AllowDBNull ?? true,
                IsCaseSensitive: false // SQL Server is case-insensitive by default
            ));
        }
        Columns = columns;

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

    public async ValueTask DisposeAsync()
    {
        if (_reader is not null) await _reader.DisposeAsync();
        await _command.DisposeAsync();
        await _connection.DisposeAsync();
    }
}
