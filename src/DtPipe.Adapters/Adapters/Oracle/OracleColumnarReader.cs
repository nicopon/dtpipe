using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Oracle.ManagedDataAccess.Client;

namespace DtPipe.Adapters.Oracle;

/// <summary>
/// Columnar stream reader for Oracle. Produces Apache Arrow RecordBatches directly
/// from OracleDataReader via typed column consumers (no boxing).
/// Implements both IStreamReader (row-mode fallback) and IColumnarStreamReader (Arrow mode).
/// </summary>
public sealed partial class OracleColumnarReader : IColumnarStreamReader, IRequiresOptions<OracleReaderOptions>
{
    private readonly OracleConnection _connection;
    private readonly OracleCommand _command;
    private OracleDataReader? _reader;
    private AdoToArrowConfig? _config;

    public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
    public Schema? Schema { get; private set; }

    [GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
    private static partial Regex FirstWordRegex();

    private static readonly string[] DdlKeywords =
    {
        "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME",
        "GRANT", "REVOKE", "COMMENT", "FLASHBACK", "PURGE",
        "INSERT", "UPDATE", "DELETE", "MERGE", "CALL",
        "LOCK", "EXECUTE", "EXPLAIN"
    };

    public OracleColumnarReader(string connectionString, string query, OracleReaderOptions options, int queryTimeout = 0)
    {
        ValidateQueryIsSafeSelect(query);
        _connection = new OracleConnection(connectionString);
        _command = new OracleCommand(query, _connection)
        {
            FetchSize = options.FetchSize,
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
        if (firstWord != "SELECT" && firstWord != "WITH")
            throw new InvalidOperationException(
                $"Only SELECT queries are allowed. Detected: {firstWord}. " +
                "DDL/DML statements are blocked for safety.");
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        await _connection.OpenAsync(ct);
        _reader = (OracleDataReader)await _command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
        Columns = ExtractColumns(_reader);

        // Build Arrow schema from PipeColumnInfo via ArrowTypeMapper — guarantees consistency
        Schema = ArrowSchemaFactory.Create(Columns);

        // Oracle maps RAW(16) to byte[], not Guid — no GuidAsBytesConsumer needed
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

        await foreach (var batch in AdoToArrow.ReadToArrowBatchesAsync(_reader, _config, cancellationToken: ct))
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

    private static List<PipeColumnInfo> ExtractColumns(OracleDataReader reader)
    {
        var columns = new List<PipeColumnInfo>(reader.FieldCount);
        var schemaTable = reader.GetSchemaTable();

        if (schemaTable is null)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var name = reader.GetName(i);
                columns.Add(new PipeColumnInfo(name, reader.GetFieldType(i), true,
                    IsCaseSensitive: name != name.ToUpperInvariant()));
            }
            return columns;
        }

        foreach (DataRow row in schemaTable.Rows)
        {
            var name = row["ColumnName"]?.ToString() ?? $"Column{columns.Count}";
            var clrType = row["DataType"] as Type ?? typeof(object);
            var allowNull = row["AllowDBNull"] as bool? ?? true;
            columns.Add(new PipeColumnInfo(name, clrType, allowNull,
                IsCaseSensitive: name != name.ToUpperInvariant()));
        }

        return columns;
    }

    public async ValueTask DisposeAsync()
    {
        if (_reader is not null) await _reader.DisposeAsync();
        await _command.DisposeAsync();
        await _connection.DisposeAsync();
    }
}
