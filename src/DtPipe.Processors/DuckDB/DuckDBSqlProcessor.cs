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
using DuckDB.NET.Native;
using Microsoft.Extensions.Logging;
using System.Runtime.InteropServices;

namespace DtPipe.Processors.DuckDB;

public sealed class DuckDBSqlProcessor : IColumnarStreamReader, IDisposable
{
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _query;
    private readonly string _mainAlias;
    private readonly string _mainChannelAlias;
    private readonly string[] _refAliases;
    private readonly string[] _refChannelAliases;
    private readonly ILogger<DuckDBSqlProcessor> _logger;

    private readonly List<IDisposable> _activeStreams = new();
    private readonly List<IntPtr> _allocatedPointers = new();

    private DuckDBConnection? _conn;
    private Schema? _resultSchema;
    private IReadOnlyList<PipeColumnInfo>? _columns;

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;
    public Schema? Schema => _resultSchema;

    public DuckDBSqlProcessor(
        IMemoryChannelRegistry registry,
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
    // Registers the --from source as an Arrow stream scan in DuckDB.
    // This allows true zero-copy data transfer from the memory channel to DuckDB.
    private async Task RegisterStreamingTableAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        ValidateSchema(channelAlias, schema);
        var channelTuple = _registry.GetArrowChannel(channelAlias)
            ?? throw new Exception($"Arrow channel '{channelAlias}' not found");

        var stream = new ChannelArrowStream(schema, channelTuple.Channel.Reader, _logger, ct);
        _activeStreams.Add(stream);

        unsafe
        {
            var ffiStreamPtr = (Apache.Arrow.C.CArrowArrayStream*)Marshal.AllocHGlobal(Marshal.SizeOf<Apache.Arrow.C.CArrowArrayStream>());
            _allocatedPointers.Add((IntPtr)ffiStreamPtr);
            Apache.Arrow.C.CArrowArrayStreamExporter.ExportArrayStream(stream, ffiStreamPtr);

            if (DuckDBArrowNativeMethods.DuckDBArrowScan(_conn!.NativeConnection, alias, ffiStreamPtr) != DuckDBState.Success)
            {
                Apache.Arrow.C.CArrowArrayStreamImporter.ImportArrayStream(ffiStreamPtr).Dispose();
                _allocatedPointers.Remove((IntPtr)ffiStreamPtr);
                Marshal.FreeHGlobal((IntPtr)ffiStreamPtr);
                throw new Exception($"Failed to register Arrow stream scan for '{alias}'");
            }
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

        DuckDBArrowNativeMethods.DuckDBArrow arrowResult;
        unsafe 
        {
            if (DuckDBArrowNativeMethods.DuckDBQueryArrow(_conn.NativeConnection, _query, out arrowResult) != DuckDBState.Success)
            {
                throw new Exception("DuckDB query_arrow execution failed.");
            }
        }

        try
        {
            await foreach (var batch in ReadRecordBatchesFromArrowResultAsync(arrowResult, ct))
                yield return batch;
        }
        finally
        {
            unsafe 
            {
                nint ptr = arrowResult.InternalPtr;
                DuckDBArrowNativeMethods.DuckDBDestroyArrow(ref ptr);
            }
        }
    }

    private async IAsyncEnumerable<RecordBatch> ReadRecordBatchesFromArrowResultAsync(
        DuckDBArrowNativeMethods.DuckDBArrow result, [EnumeratorCancellation] CancellationToken ct)
    {
        Schema? arrowSchema = GetSchemaFromResult(result);
        if (arrowSchema == null) yield break;

        while (true)
        {
            RecordBatch? batch = FetchNextBatch(result, arrowSchema);
            if (batch == null) break;
            yield return batch;
        }
    }

    private unsafe Schema? GetSchemaFromResult(DuckDBArrowNativeMethods.DuckDBArrow result)
    {
        Apache.Arrow.C.CArrowSchema ffiSchema = default;
        Apache.Arrow.C.CArrowSchema* pSchema = &ffiSchema;
        // DuckDB fills the struct pointed to by pSchema
        if (DuckDBArrowNativeMethods.DuckDBQueryArrowSchema(result, (nint*)&pSchema) == DuckDBState.Success)
        {
            // The importer will throw if release is null, so we just try to import.
            try
            {
                return Apache.Arrow.C.CArrowSchemaImporter.ImportSchema(&ffiSchema);
            }
            catch (ArgumentException)
            {
                return null;
            }
        }
        return null;
    }

    private unsafe RecordBatch? FetchNextBatch(DuckDBArrowNativeMethods.DuckDBArrow result, Schema schema)
    {
        Apache.Arrow.C.CArrowArray ffiArray = default;
        Apache.Arrow.C.CArrowArray* pArray = &ffiArray;

        if (DuckDBArrowNativeMethods.DuckDBQueryArrowArray(result, (nint*)&pArray) != DuckDBState.Success) return null;

        // Note: Field 'release' is internal, but the importer checks it.
        try
        {
            return Apache.Arrow.C.CArrowArrayImporter.ImportRecordBatch(&ffiArray, schema);
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    public void Dispose()
    {
        foreach (var ptr in _allocatedPointers)
        {
            Marshal.FreeHGlobal(ptr);
        }
        _allocatedPointers.Clear();

        foreach (var stream in _activeStreams)
        {
            stream.Dispose();
        }
        _activeStreams.Clear();

        _conn?.Dispose();
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

    private static Type ToDuckDBCompatibleType(Type clrType)
        => clrType == typeof(byte[]) ? typeof(Guid) : clrType;

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
                ArrowTypeId.Binary => "BLOB",
                ArrowTypeId.Date32 => "DATE",
                ArrowTypeId.Date64 => "TIMESTAMP",
                ArrowTypeId.Timestamp => "TIMESTAMP",
                ArrowTypeId.Duration => "BIGINT",
                ArrowTypeId.Decimal256 => "DOUBLE",
                _ => "VARCHAR",
            }
        };
    }

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

    private void ValidateAliases()
        => SqlProcessorHelpers.ValidateAliases(_mainChannelAlias, _refChannelAliases);

    private static void ValidateSchema(string alias, Schema schema)
        => SqlProcessorHelpers.ValidateSchema(alias, schema);

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
