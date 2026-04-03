#pragma warning disable DuckDBNET001 // RegisterTableFunction and IDuckDBDataWriter are experimental in DuckDB.NET
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Processors.Sql;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;

namespace DtPipe.Processors.DuckDB;

public sealed class DuckDBSqlProcessor : IColumnarStreamReader
{
    private readonly IArrowChannelRegistry _registry;
    private readonly string _query;
    private readonly string _mainAlias;
    private readonly string _mainChannelAlias;
    private readonly string[] _refAliases;
    private readonly string[] _refChannelAliases;
    private readonly ILogger<DuckDBSqlProcessor> _logger;

    private DuckDBConnection? _conn;
    private Schema? _resultSchema;
    private IReadOnlyList<PipeColumnInfo>? _columns;

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;
    public Schema? Schema => _resultSchema;

    public DuckDBSqlProcessor(
        IArrowChannelRegistry registry,
        string query,
        string mainAlias,
        string mainChannelAlias,
        string[] refAliases,
        string[] refChannelAliases,
        ILogger<DuckDBSqlProcessor> logger)
    {
        _registry = registry;
        _query = query;
        _mainAlias = mainAlias;
        _mainChannelAlias = mainChannelAlias;
        _refAliases = refAliases;
        _refChannelAliases = refChannelAliases;
        _logger = logger;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        _logger.LogDebug("DuckDBSqlProcessor: OpenAsync — query={Query}", _query);
        try
        {
            _conn = new DuckDBConnection("DataSource=:memory:");
            await _conn.OpenAsync(ct);

            ValidateAliases();

            if (_refAliases.Length > 0)
            {
                var tasks = _refAliases
                    .Select((alias, i) => RegisterRefTableAsync(alias, _refChannelAliases[i], ct))
                    .ToList();
                await Task.WhenAll(tasks);
            }

            if (!string.IsNullOrEmpty(_mainAlias))
                await RegisterStreamingTableAsync(_mainAlias, _mainChannelAlias, ct);

            _logger.LogDebug("DuckDBSqlProcessor: All sources registered. Inspecting schema...");
            _resultSchema = await InspectSchemaAsync(ct);
            _columns = _resultSchema.FieldsList
                .Select(f => new PipeColumnInfo(f.Name, ArrowTypeMapper.GetClrTypeFromField(f), f.IsNullable))
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DuckDBSqlProcessor: OpenAsync FAILED: {Message}", ex.Message);
            throw;
        }
    }

    // Materialises a --ref source fully into an in-memory DuckDB table before query execution.
    private async Task RegisterRefTableAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        ValidateSchema(channelAlias, schema);
        var channelTuple = _registry.GetArrowChannel(channelAlias)
            ?? throw new Exception($"Arrow channel '{channelAlias}' not found");

        using var createCmd = _conn!.CreateCommand();
        createCmd.CommandText = BuildCreateTableSql(alias, schema);
        await createCmd.ExecuteNonQueryAsync(ct);

        using var appender = _conn.CreateAppender(alias);
        await foreach (var batch in channelTuple.Channel.Reader.ReadAllAsync(ct))
        {
            for (int row = 0; row < batch.Length; row++)
            {
                var appenderRow = appender.CreateRow();
                for (int col = 0; col < batch.ColumnCount; col++)
                {
                    var column = batch.Column(col);
                    if (column.IsNull(row))
                        appenderRow.AppendNullValue();
                    else
                        AppendArrowValue(appenderRow, column, row);
                }
                appenderRow.EndRow();
            }
        }
        appender.Close();
    }

    // Registers the --from source as a DuckDB table function backed by the Arrow channel,
    // then exposes it as a plain VIEW so user SQL can reference it as a regular table name
    // (without parentheses). DuckDB pulls rows lazily through the IEnumerable —
    // the channel is never fully buffered.
    private async Task RegisterStreamingTableAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        ValidateSchema(channelAlias, schema);
        var channelTuple = _registry.GetArrowChannel(channelAlias)
            ?? throw new Exception($"Arrow channel '{channelAlias}' not found");

        var channelReader = channelTuple.Channel.Reader;
        var columnCount = schema.FieldsList.Count;

        var columns = schema.FieldsList
            .Select(f => new global::DuckDB.NET.Data.ColumnInfo(
                f.Name.ToLowerInvariant(),
                ToDuckDBCompatibleType(ArrowTypeMapper.GetClrTypeFromField(f))))
            .ToArray();

        // DuckDB table functions are called as tablefn(), not tablefn — use a hidden name and
        // expose a VIEW with the logical alias so user SQL can write "FROM alias" normally.
        var fnName = $"__dtpipe_stream_{alias}";

        _conn!.RegisterTableFunction(
            fnName,
            () => new global::DuckDB.NET.Data.TableFunction(columns, LazyChannelRows(channelReader, ct)),
            (item, writers, rowInChunk) =>
            {
                var (batch, rowIndex) = ((RecordBatch, int))item!;
                for (int c = 0; c < columnCount; c++)
                {
                    var col = batch.Column(c);
                    if (col.IsNull(rowIndex))
                        writers[c].WriteNull(rowInChunk);
                    else
                        WriteTypedValue(writers[c], ArrowTypeMapper.GetValue(col, rowIndex), rowInChunk);
                }
            });

        using var viewCmd = _conn.CreateCommand();
        viewCmd.CommandText = $"CREATE VIEW \"{alias}\" AS SELECT * FROM \"{fnName}\"()";
        await viewCmd.ExecuteNonQueryAsync(ct);
    }

    // Lazy enumerator: yields (batch, rowIndex) pairs from the Arrow channel without materialising anything.
    // Called synchronously from DuckDB's execution thread — WaitToReadAsync is synchronously awaited.
    private static IEnumerable<(RecordBatch Batch, int RowIndex)> LazyChannelRows(
        ChannelReader<RecordBatch> reader, CancellationToken ct)
    {
        while (true)
        {
            if (!reader.TryRead(out var batch))
            {
                var hasMore = reader.WaitToReadAsync(ct).AsTask().GetAwaiter().GetResult();
                if (!hasMore) yield break;
                if (!reader.TryRead(out batch)) continue;
            }

            if (batch is null) continue;
            for (int r = 0; r < batch.Length; r++)
                yield return (batch, r);
        }
    }

    private async Task<Schema> InspectSchemaAsync(CancellationToken ct)
    {
        using var cmd = _conn!.CreateCommand();
        cmd.CommandText = $"SELECT * FROM ({_query}) __schema_probe LIMIT 0";
        using var reader = (DuckDBDataReader)await cmd.ExecuteReaderAsync(ct);
        var config = new AdoToArrowConfigBuilder().Build();
        return AdoToArrowUtils.CreateSchema(reader, config);
    }

    // DtPipe UUID convention for Guid is natively handled by the centralized ArrowTypeMap via AdoToArrowUtils.

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_conn is null) yield break;

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = _query;
        cmd.UseStreamingMode = true;
        using var reader = (DuckDBDataReader)await cmd.ExecuteReaderAsync(ct);

        var config = new AdoToArrowConfigBuilder()
            .SetTargetBatchSize(65536)
            .Build();

        // DuckDB returns Guid for UUID columns. DtPipeTypeResolver maps them to FixedSizeBinaryType(16).
        // GuidToBinaryConsumer reads Guid via GetGuid() and writes as RFC 4122 bytes (16 bytes).
        Apache.Arrow.Ado.IAdoConsumer factory(IArrowType arrowType, int colIdx)
        {
            if (arrowType is FixedSizeBinaryType && reader.GetFieldType(colIdx) == typeof(Guid))
                return new GuidToBinaryConsumer(colIdx);
            return Apache.Arrow.Ado.AdoConsumerFactory.Create(arrowType, colIdx);
        }

        await foreach (var batch in AdoToArrow.ReadToArrowBatchesAsync(reader, config, factory, ct))
            yield return batch;
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var recordBatch in ReadRecordBatchesAsync(ct))
        {
            using (recordBatch)
                yield return ConvertBatchToRows(recordBatch);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_conn is not null) { await _conn.DisposeAsync(); _conn = null; }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static ReadOnlyMemory<object?[]> ConvertBatchToRows(RecordBatch batch)
        => SqlProcessorHelpers.ConvertBatchToRows(batch);

    // Maps CLR types that DuckDB.NET's RegisterTableFunction doesn't support (e.g. byte[])
    // to the closest supported equivalent. byte[] (Arrow Binary) represents UUIDs in practice;
    // map to Guid so DuckDB stores the column as UUID for proper JOIN compatibility.
    private static Type ToDuckDBCompatibleType(Type clrType)
        => clrType == typeof(byte[]) ? typeof(Guid) : clrType;

    // Builds a CREATE TABLE statement from an Arrow schema for --ref materialization.
    private static string BuildCreateTableSql(string tableName, Schema schema)
    {
        var cols = schema.FieldsList.Select(f =>
            $"\"{f.Name.ToLowerInvariant()}\" {ArrowTypeToDuckDbSql(f)}{(f.IsNullable ? "" : " NOT NULL")}");
        return $"CREATE TABLE \"{tableName}\" ({string.Join(", ", cols)})";
    }

    private static string ArrowTypeToDuckDbSql(Field field) 
    {
        if (ArrowTypeMapper.GetClrTypeFromField(field) == typeof(Guid))
            return "UUID";

        return field.DataType switch
        {
            Decimal128Type d => $"DECIMAL({d.Precision},{d.Scale})",
            FixedSizeBinaryType => "BLOB",
            _ => field.DataType.TypeId switch
            {
                ArrowTypeId.Boolean => "BOOLEAN",
                ArrowTypeId.Int8 => "TINYINT",
                ArrowTypeId.UInt8 => "UTINYINT",
                ArrowTypeId.Int16 => "SMALLINT",
                ArrowTypeId.UInt16 => "USMALLINT",
                ArrowTypeId.Int32 => "INTEGER",
                ArrowTypeId.UInt32 => "UINTEGER",
                ArrowTypeId.Int64 => "BIGINT",
                ArrowTypeId.UInt64 => "UBIGINT",
                ArrowTypeId.Float => "FLOAT",
                ArrowTypeId.Double => "DOUBLE",
                ArrowTypeId.String => "VARCHAR",
                ArrowTypeId.Binary => "BLOB",  // No legacy UUID mapping here mapping here either without arrow.uuid metadata
                ArrowTypeId.Date32 => "DATE",
                ArrowTypeId.Date64 => "TIMESTAMP",
                ArrowTypeId.Timestamp => "TIMESTAMP",
                ArrowTypeId.Duration => "BIGINT",
                ArrowTypeId.Decimal256 => "DOUBLE",
                _ => "VARCHAR",
            }
        };
    }

    // Writes an Arrow array value to a DuckDB appender row for --ref table insertion.
    private static void AppendArrowValue(IDuckDBAppenderRow row, IArrowArray column, int rowIndex)
    {
        var val = ArrowTypeMapper.GetValue(column, rowIndex);
        switch (val)
        {
            case null: row.AppendNullValue(); break;
            case bool b: row.AppendValue(b); break;
            case sbyte sb: row.AppendValue(sb); break;
            case byte by: row.AppendValue(by); break;
            case short s: row.AppendValue(s); break;
            case ushort us: row.AppendValue(us); break;
            case int i: row.AppendValue(i); break;
            case uint ui: row.AppendValue(ui); break;
            case long l: row.AppendValue(l); break;
            case ulong ul: row.AppendValue(ul); break;
            case float f: row.AppendValue(f); break;
            case double d: row.AppendValue(d); break;
            case decimal dec: row.AppendValue(dec); break;
            case string str: row.AppendValue(str); break;
            case DateTime dt: row.AppendValue(dt); break;
            case DateTimeOffset dto: row.AppendValue(dto.DateTime); break;
            case TimeSpan ts: row.AppendValue(ts); break;
            case Guid g: row.AppendValue(g); break;
            case byte[] bytes: row.AppendValue((IEnumerable<byte>)bytes); break;
            default: row.AppendValue(val.ToString() ?? string.Empty); break;
        }
    }

    // Writes a value to a DuckDB table-function writer (used for the streaming --from source).
    private static void WriteTypedValue(global::DuckDB.NET.Data.DataChunk.Writer.IDuckDBDataWriter writer, object? val, ulong rowIndex)
    {
        switch (val)
        {
            case null: writer.WriteNull(rowIndex); break;
            case bool b: writer.WriteValue(b, rowIndex); break;
            case sbyte sb: writer.WriteValue(sb, rowIndex); break;
            case byte by: writer.WriteValue(by, rowIndex); break;
            case short s: writer.WriteValue(s, rowIndex); break;
            case ushort us: writer.WriteValue(us, rowIndex); break;
            case int i: writer.WriteValue(i, rowIndex); break;
            case uint ui: writer.WriteValue(ui, rowIndex); break;
            case long l: writer.WriteValue(l, rowIndex); break;
            case ulong ul: writer.WriteValue(ul, rowIndex); break;
            case float f: writer.WriteValue(f, rowIndex); break;
            case double d: writer.WriteValue(d, rowIndex); break;
            case decimal dec: writer.WriteValue(dec, rowIndex); break;
            case string str: writer.WriteValue(str, rowIndex); break;
            case DateTime dt: writer.WriteValue(dt, rowIndex); break;
            case DateTimeOffset dto: writer.WriteValue(dto.DateTime, rowIndex); break;
            case TimeSpan ts: writer.WriteValue(ts, rowIndex); break;
            case Guid g: writer.WriteValue(g, rowIndex); break;
            case byte[] bytes: writer.WriteValue(bytes, rowIndex); break;
            default: writer.WriteValue(val.ToString() ?? string.Empty, rowIndex); break;
        }
    }

    private void ValidateAliases()
        => SqlProcessorHelpers.ValidateAliases(_mainChannelAlias, _refChannelAliases);

    private static void ValidateSchema(string alias, Schema schema)
        => SqlProcessorHelpers.ValidateSchema(alias, schema);

    // Reads a DuckDB UUID column returned as System.Guid, stores as FixedSizeBinary(16) RFC 4122.
    // The arrow.uuid Field metadata is attached at schema creation time (ArrowSchemaFactory).
    private sealed class GuidToBinaryConsumer : Apache.Arrow.Ado.IAdoConsumer
    {
        private readonly int _colIdx;
        private readonly Apache.Arrow.Serialization.Reflection.FixedSizeBinaryArrayBuilder _builder = new(16);

        public GuidToBinaryConsumer(int colIdx) { _colIdx = colIdx; }

        public IArrowType ArrowType => new FixedSizeBinaryType(16);

        public void Consume(System.Data.Common.DbDataReader reader)
        {
            if (reader.IsDBNull(_colIdx))
                _builder.AppendNull();
            else
                _builder.Append(ArrowTypeMapper.ToArrowUuidBytes(((DuckDBDataReader)reader).GetGuid(_colIdx)));
        }

        public IArrowArray BuildArray() => _builder.Build();
        public void Reset() => _builder.Clear();
        public void Dispose() { }
    }
}
