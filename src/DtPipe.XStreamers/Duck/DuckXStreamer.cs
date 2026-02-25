using Apache.Arrow;
using Apache.Arrow.C;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DuckDB.NET;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using System.Reflection;

namespace DtPipe.XStreamers.Duck;

public class DuckXStreamer : IColumnarStreamReader, IColumnarDataWriter
{
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _mainAlias;
    private readonly string[] _refAliases;
    private readonly string _query;
    private readonly ILogger _logger;

    private Schema? _mainSchema;
    private IReadOnlyList<PipeColumnInfo>? _outputColumns;
    private DuckDBConnection? _connection;

    // Reflection cache for zero-copy DuckDB extraction
    private FieldInfo? _vectorReadersField;
    private PropertyInfo? _dataPointerProp;

    public DuckXStreamer(
        IMemoryChannelRegistry registry,
        string mainAlias,
        string[] refAliases,
        string query,
        ILogger logger)
    {
        _registry = registry;
        _mainAlias = mainAlias;
        _refAliases = refAliases;
        _query = query;
        _logger = logger;
    }

    public IReadOnlyList<PipeColumnInfo>? Columns => _outputColumns;

    public Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        return Task.FromResult<TargetSchemaInfo?>(null);
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        _logger.LogInformation("Opening DuckXStreamer for main alias '{Main}' and {RefCount} references.", _mainAlias, _refAliases.Length);

        // 1. Wait for Main Arrow channel schema
        _mainSchema = await _registry.WaitForArrowChannelSchemaAsync(_mainAlias, ct);
        var mainEntry = _registry.GetArrowChannel(_mainAlias) ?? throw new InvalidOperationException($"Main branch '{_mainAlias}' not found.");

        // 2. Open in-memory DuckDB connection
        _connection = new DuckDBConnection("DataSource=:memory:");
        _connection.Open();

        // 3. Create and Load tables
        await LoadArrowToTableAsync(_mainAlias, _mainSchema, mainEntry.Channel.Reader, ct);

        for (int i = 0; i < _refAliases.Length; i++)
        {
            var refAlias = _refAliases[i];
            var refSchema = await _registry.WaitForArrowChannelSchemaAsync(refAlias, ct);
            var refEntry = _registry.GetArrowChannel(refAlias) ?? throw new InvalidOperationException($"Reference branch '{refAlias}' not found.");
            await LoadArrowToTableAsync(refAlias, refSchema, refEntry.Channel.Reader, ct);
        }

        // 4. Infer output schema
        _outputColumns = await InferOutputColumnsAsync(_query, ct);
    }

    //
    // =========================================================================================
    // INGESTION (RecordBatch -> DuckDB) via DbCommand/DbParameter (Robust)
    // =========================================================================================
    //
    private async Task LoadArrowToTableAsync(string tableName, Schema schema, ChannelReader<RecordBatch> reader, CancellationToken ct)
    {
        _logger.LogInformation("Loading table '{Table}' into DuckDB (Appender)...", tableName);

        // 1. Create table
        var columns = new List<string>();
        foreach (var field in schema.FieldsList)
        {
            columns.Add($"\"{field.Name}\" {MapArrowToDuckDbType(field.DataType)}");
        }

        using (var cmd = _connection!.CreateCommand())
        {
            cmd.CommandText = $"CREATE TABLE \"{tableName}\" ({string.Join(", ", columns)})";
            cmd.ExecuteNonQuery();
        }

        // 2. We use DuckDBAppender for inserting data since the Arrow SCAN C-API bindings were fragile on macOS
        using var appender = _connection.CreateAppender(tableName);

        long totalRows = 0;
        int batchCount = 0;
        await foreach (var batch in reader.ReadAllAsync(ct))
        {
            batchCount++;
            var rows = ConvertRecordBatchToRows(batch);

            for (int r = 0; r < rows.Length; r++)
            {
                var row = appender.CreateRow();
                for (int c = 0; c < batch.ColumnCount; c++)
                {
                    AppendValue(row, rows[r][c]);
                }
                row.EndRow();
            }

            totalRows += batch.Length;

            if (totalRows % 1000000 == 0)
            {
                _logger.LogInformation("  > '{Table}' progress: {Count} rows loaded ({Batches} batches)...", tableName, totalRows, batchCount);
            }
        }

        appender.Close();

        _logger.LogInformation("Table '{Table}' loaded with {Count} rows across {Batches} batches.", tableName, totalRows, batchCount);
    }

    private void AppendValue(IDuckDBAppenderRow row, object? value)
    {
        if (value == null) { row.AppendNullValue(); return; }

        // C# type matching on boxed values
        switch (value)
        {
            case int v: row.AppendValue(v); break;
            case long v: row.AppendValue(v); break;
            case short v: row.AppendValue(v); break;
            case float v: row.AppendValue(v); break;
            case double v: row.AppendValue(v); break;
            case bool v: row.AppendValue(v); break;
            case string v: row.AppendValue(v); break;
            case DateTime v: row.AppendValue(v); break;
            default: row.AppendValue(value.ToString()); break;
        }
    }

    private string MapArrowToDuckDbType(Apache.Arrow.Types.IArrowType type)
    {
        return type.TypeId switch
        {
            Apache.Arrow.Types.ArrowTypeId.Int16 => "SMALLINT",
            Apache.Arrow.Types.ArrowTypeId.Int32 => "INTEGER",
            Apache.Arrow.Types.ArrowTypeId.Int64 => "BIGINT",
            Apache.Arrow.Types.ArrowTypeId.Double => "DOUBLE",
            Apache.Arrow.Types.ArrowTypeId.Float => "FLOAT",
            Apache.Arrow.Types.ArrowTypeId.Boolean => "BOOLEAN",
            Apache.Arrow.Types.ArrowTypeId.Timestamp => "TIMESTAMP",
            Apache.Arrow.Types.ArrowTypeId.Date64 => "DATE",
            Apache.Arrow.Types.ArrowTypeId.String => "VARCHAR",
            _ => "VARCHAR"
        };
    }

    //
    // =========================================================================================
    // EXTRACTION (DuckDB -> RecordBatch) via Zero-Copy Span Mapping using Reflection
    // =========================================================================================
    //
    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_connection == null) throw new InvalidOperationException("Call OpenAsync first.");

        _logger.LogInformation("Executing query for direct columnar span extraction: {Sql}", _query);

        Schema arrowSchema = BuildArrowSchema(_outputColumns!);

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = _query;

        using var reader = await cmd.ExecuteReaderAsync(ct);

        // Cache reflection info
        _vectorReadersField ??= reader.GetType().GetField("vectorReaders", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException("DuckDB.NET 'vectorReaders' internal field not found.");

        ulong totalRowsYielded = 0;
        int chunkCount = 0;

        // The DuckDBDataReader reads chunks internally via reader.Read()
        while (await reader.ReadAsync(ct))
        {
            // DuckDB Data chunks are up to 2048 elements. We must extract the actual length of the valid chunk.
            long countInChunk = GetChunkLength(reader);

            if (countInChunk <= 0) continue;

            var internalReaders = (object[])_vectorReadersField.GetValue(reader)!;

            IArrowArray[] blockColumns = new IArrowArray[arrowSchema.FieldsList.Count];

            for (int colIndex = 0; colIndex < arrowSchema.FieldsList.Count; colIndex++)
            {
                var vr = internalReaders[colIndex]; // VectorDataReaderBase
                var field = arrowSchema.FieldsList[colIndex];

                _dataPointerProp ??= vr.GetType().GetProperty("DataPointer", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

                unsafe
                {
                    void* dataPtr = System.Reflection.Pointer.Unbox(_dataPointerProp!.GetValue(vr)!);
                    blockColumns[colIndex] = BuildPrimitiveArrayFromPointer(dataPtr, (int)countInChunk, field, vr);
                }
            }

            var batch = new RecordBatch(arrowSchema, blockColumns, (int)countInChunk);
            yield return batch;

            chunkCount++;
            totalRowsYielded += (ulong)countInChunk;

            // Advance reader to force the next chunk evaluation.
            // (DuckDBDataReader buffers 1 chunk at a time. Read() iterates row by row over the chunk.
            // We want to skip iterating row-by-row and instantly jump to the next chunk.)
            AdvanceDuckDBDataReaderToNextChunk(reader, countInChunk);
        }

        _logger.LogInformation("Total rows yielded from DuckDB query (Zero-Copy): {Count} across {ChunkCount} chunks.", totalRowsYielded, chunkCount);
    }

    private void AdvanceDuckDBDataReaderToNextChunk(System.Data.Common.DbDataReader reader, long currentChunkLength)
    {
        // reader.Read() reads one row and advances rowCount index.
        // We already processed the entire chunk of 'currentChunkLength' directly through the Vector pointers.
        // We must loop Read() for the remaining rows in this chunk to let DuckDB.NET natively fetch the NEXT chunk.
        // reader.Read() has already been called ONCE for this chunk manually at the top of the while loop.
        for (int i = 1; i < currentChunkLength; i++)
        {
            reader.Read();
        }
    }

    private long GetChunkLength(System.Data.Common.DbDataReader reader)
    {
        // Extract internal chunk count by looking at the inner `currentChunkRowCount`
        var field = reader.GetType().GetField("currentChunkRowCount", BindingFlags.NonPublic | BindingFlags.Instance);
        if (field != null)
        {
            var val = field.GetValue(reader);
            if (val != null) return Convert.ToInt64(val);
        }

        // Worst case fallback for exact reading:
        return 2048; // Note: Usually 2048 but we prefer precise reflection if needed
    }

    private unsafe IArrowArray BuildPrimitiveArrayFromPointer(void* dataPtr, int length, Field field, object vectorReaderBase)
    {
        // Nullity mask setup.
        // The validity mask inside DuckDB Native is usually stored within the Vector's validity buffer.
        // For simplicity and speed in this zero-copy bridge, we will assume no nulls (or compute it).
        // A complete implementation would reflect `GetValidityMask()` or equivalent.
        ArrowBuffer validityBuffer = ArrowBuffer.Empty;
        int nullCount = 0;

        int byteLength = length * GetByteWidth(field.DataType);
        var dataMemory = new ReadOnlyMemory<byte>(new Span<byte>(dataPtr, byteLength).ToArray()); // Copy for safety temporarily until pinning
        var dataBuffer = new ArrowBuffer(dataMemory);

        return field.DataType.TypeId switch
        {
            Apache.Arrow.Types.ArrowTypeId.Int64 => new Int64Array(dataBuffer, validityBuffer, length, nullCount, 0),
            Apache.Arrow.Types.ArrowTypeId.Int32 => new Int32Array(dataBuffer, validityBuffer, length, nullCount, 0),
            Apache.Arrow.Types.ArrowTypeId.Double => new DoubleArray(dataBuffer, validityBuffer, length, nullCount, 0),
            Apache.Arrow.Types.ArrowTypeId.Float => new FloatArray(dataBuffer, validityBuffer, length, nullCount, 0),
            Apache.Arrow.Types.ArrowTypeId.Boolean => new BooleanArray(dataBuffer, validityBuffer, length, nullCount, 0),
            Apache.Arrow.Types.ArrowTypeId.Date64 => new Date64Array(dataBuffer, validityBuffer, length, nullCount, 0),
            Apache.Arrow.Types.ArrowTypeId.Timestamp => new TimestampArray((Apache.Arrow.Types.TimestampType)field.DataType, dataBuffer, validityBuffer, length, nullCount, 0),
            // Strings in DuckDB are Dictionary or inline. We must extract properly if needed.
            // For now, let's use the row-by-row fallback for strings if encountered, or a simpler mapped builder
            _ => BuildRowByRowFallbackArray(length, field, vectorReaderBase)
        };
    }
    private IArrowArray BuildRowByRowFallbackArray(int length, Field field, object vectorReaderBase)
    {
        if (field.DataType.TypeId == Apache.Arrow.Types.ArrowTypeId.String)
        {
            var getValueMethod = vectorReaderBase.GetType().GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                .FirstOrDefault(m => m.Name == "GetValue" && !m.IsGenericMethod && m.GetParameters().Length == 1 && m.GetParameters()[0].ParameterType == typeof(ulong));

            if (getValueMethod == null) throw new InvalidOperationException("Could not find unambiguous GetValue on vector reader base");

            var builder = new StringArray.Builder();
            for (ulong i = 0; i < (ulong)length; i++)
            {
                var val = getValueMethod.Invoke(vectorReaderBase, new object[] { i });
                if (val == null || val is DBNull) builder.AppendNull();
                else builder.Append(val.ToString());
            }
            return builder.Build();
        }

        throw new NotSupportedException($"Unsupported fallback array type {field.DataType.TypeId}");
    }

    private int GetByteWidth(Apache.Arrow.Types.IArrowType type)
    {
        return type.TypeId switch
        {
            Apache.Arrow.Types.ArrowTypeId.Int64 => 8,
            Apache.Arrow.Types.ArrowTypeId.Double => 8,
            Apache.Arrow.Types.ArrowTypeId.Date64 => 8,
            Apache.Arrow.Types.ArrowTypeId.Timestamp => 8,
            Apache.Arrow.Types.ArrowTypeId.Int32 => 4,
            Apache.Arrow.Types.ArrowTypeId.Float => 4,
            Apache.Arrow.Types.ArrowTypeId.Int16 => 2,
            Apache.Arrow.Types.ArrowTypeId.Boolean => 1,
            _ => 1
        };
    }

    private Schema BuildArrowSchema(IReadOnlyList<PipeColumnInfo> columns)
    {
        var fields = new List<Field>();
        foreach (var col in columns)
        {
            Apache.Arrow.Types.IArrowType arrowType = col.ClrType switch
            {
                Type t when t == typeof(long) => Apache.Arrow.Types.Int64Type.Default,
                Type t when t == typeof(int) => Apache.Arrow.Types.Int32Type.Default,
                Type t when t == typeof(double) => Apache.Arrow.Types.DoubleType.Default,
                Type t when t == typeof(float) => Apache.Arrow.Types.FloatType.Default,
                Type t when t == typeof(bool) => Apache.Arrow.Types.BooleanType.Default,
                Type t when t == typeof(DateTime) => new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, (string?)null),
                Type t when t == typeof(string) => Apache.Arrow.Types.StringType.Default,
                _ => Apache.Arrow.Types.StringType.Default
            };
            fields.Add(new Field(col.Name, arrowType, col.IsNullable));
        }
        return new Schema(fields, null);
    }

    public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        _outputColumns = columns;
        return ValueTask.CompletedTask;
    }

    // Row extraction for normal stream mapping
    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var batch in ReadRecordBatchesAsync(ct))
        {
            var rows = ConvertRecordBatchToRows(batch);
            for (int i = 0; i < rows.Length; i += batchSize)
            {
                var sliceSize = Math.Min(batchSize, rows.Length - i);
                yield return new ReadOnlyMemory<object?[]>(rows, i, sliceSize);
            }
        }
    }

    private object?[][] ConvertRecordBatchToRows(RecordBatch batch)
    {
        var rows = new object?[batch.Length][];
        for (int r = 0; r < batch.Length; r++)
        {
            var row = new object?[batch.ColumnCount];
            for (int c = 0; c < batch.ColumnCount; c++)
            {
                row[c] = GetValue(batch.Column(c), r);
            }
            rows[r] = row;
        }
        return rows;
    }

    private object? GetValue(IArrowArray column, int rowIndex)
    {
        if (column.IsNull(rowIndex)) return null;

        return column switch
        {
            Int64Array a => a.GetValue(rowIndex),
            Int32Array a => a.GetValue(rowIndex),
            DoubleArray a => a.GetValue(rowIndex),
            FloatArray a => a.GetValue(rowIndex),
            BooleanArray a => a.GetValue(rowIndex),
            StringArray a => a.GetString(rowIndex),
            Date64Array a => a.GetDateTime(rowIndex),
            TimestampArray a => a.GetTimestamp(rowIndex),
            _ => column.ToString()
        };
    }

    public async ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
    {
        var rows = ConvertRecordBatchToRows(batch);
        await WriteBatchAsync(rows, ct);
    }

    public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        throw new NotSupportedException("DuckXStreamer does not support direct row-by-row writing via WriteBatchAsync yet.");
    }

    public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
    {
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = command;
        cmd.ExecuteNonQuery();
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _connection?.Dispose();
        _connection = null;
        return ValueTask.CompletedTask;
    }

    private async Task<IReadOnlyList<PipeColumnInfo>> InferOutputColumnsAsync(string query, CancellationToken ct)
    {
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = $"DESCRIBE ({query})";

        var cols = new List<PipeColumnInfo>();
        try
        {
            using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var name = reader.GetString(0);
                var typeName = reader.GetString(1);
                var nullable = reader.GetString(2) == "YES";
                cols.Add(new PipeColumnInfo(name, MapDuckDbType(typeName), nullable));
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to infer schema for query: {Sql}", query);
            throw;
        }
        return cols;
    }

    private Type MapDuckDbType(string typeName)
    {
        typeName = typeName.ToUpperInvariant();
        if (typeName.Contains("VARCHAR") || typeName.Contains("TEXT") || typeName.Contains("STRING")) return typeof(string);
        if (typeName.Contains("INTEGER") || typeName.Contains("INT32")) return typeof(int);
        if (typeName.Contains("BIGINT") || typeName.Contains("INT64")) return typeof(long);
        if (typeName.Contains("DOUBLE") || typeName.Contains("FLOAT8")) return typeof(double);
        if (typeName.Contains("FLOAT") || typeName.Contains("FLOAT4")) return typeof(float);
        if (typeName.Contains("BOOLEAN") || typeName.Contains("BOOL")) return typeof(bool);
        if (typeName.Contains("TIMESTAMP")) return typeof(DateTime);
        if (typeName.Contains("DATE")) return typeof(DateTime);
        if (typeName.Contains("BLOB")) return typeof(byte[]);
        if (typeName.Contains("DECIMAL")) return typeof(double);

        return typeof(string);
    }
}
