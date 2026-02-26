using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace DtPipe.Adapters.Infrastructure.Arrow;

/// <summary>
/// Implementation of IRowToColumnarBridge that converts C# object?[] rows
/// into Apache Arrow RecordBatches.
/// </summary>
public sealed class ArrowRowToColumnarBridge : IRowToColumnarBridge
{
    private readonly ILogger _logger;
    private readonly Channel<RecordBatch> _outputChannel;

    private Schema? _schema;
    private List<IArrowArrayBuilder>? _builders;
    private Action<object?>[]? _appenders;
    private int _batchSize;
    private int _rowsInBuffer;
    private bool _isComplete;

    public ArrowRowToColumnarBridge(ILogger? logger = null)
    {
        _logger = logger ?? NullLogger.Instance;
        // Bounded channel to prevent over-allocation if consumer is slow
        _outputChannel = Channel.CreateBounded<RecordBatch>(new BoundedChannelOptions(10)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false
        });
    }

    public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, int batchSize, CancellationToken ct = default)
    {
        _batchSize = batchSize;
        _schema = BuildSchema(columns);
        _builders = CreateBuilders(_schema);
        _appenders = CreateAppenders(_builders);
        _rowsInBuffer = 0;

        _logger.LogDebug("Arrow Bridge initialized with {Count} columns, batch size {BatchSize}", columns.Count, batchSize);
        return ValueTask.CompletedTask;
    }

    public async ValueTask IngestRowsAsync(ReadOnlyMemory<object?[]> rows, CancellationToken ct = default)
    {
        if (_appenders == null || _schema == null)
            throw new InvalidOperationException("Call InitializeAsync first.");

        var appenders = _appenders;
        int i = 0;
        int totalRows = rows.Length;

        while (i < totalRows)
        {
            int spaceInBatch = _batchSize - _rowsInBuffer;
            int toProcess = Math.Min(totalRows - i, spaceInBatch);

            // Process a chunk without await
            ProcessChunk(rows.Slice(i, toProcess).Span, appenders);

            _rowsInBuffer += toProcess;
            i += toProcess;

            if (_rowsInBuffer >= _batchSize)
            {
                await FlushCurrentBatchAsync(ct);
            }
        }
    }

    private void ProcessChunk(ReadOnlySpan<object?[]> chunk, Action<object?>[] appenders)
    {
        for (int j = 0; j < chunk.Length; j++)
        {
            var row = chunk[j];
            for (int colIdx = 0; colIdx < row.Length; colIdx++)
            {
                appenders[colIdx](row[colIdx]);
            }
        }
    }

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var batch in _outputChannel.Reader.ReadAllAsync(ct))
        {
            yield return batch;
        }
    }

    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        if (_isComplete) return;

        await FlushCurrentBatchAsync(ct);
        _outputChannel.Writer.TryComplete();
        _isComplete = true;
    }

    private async Task FlushCurrentBatchAsync(CancellationToken ct)
    {
        if (_builders == null || _schema == null || _rowsInBuffer == 0) return;

        var arrays = new List<IArrowArray>();
        for (int i = 0; i < _builders.Count; i++)
        {
            var arr = BuildArray(_builders[i]);
            Console.WriteLine($"[DEBUG] Bridge: Built array {i} for field '{_schema.FieldsList[i].Name}', Type={arr.GetType().Name}, Length={arr.Length}");
            arrays.Add(arr);
        }

        var batch = new RecordBatch(_schema, arrays, _rowsInBuffer);
        Console.WriteLine($"[DEBUG] Bridge: Writing batch to channel (Length={_rowsInBuffer})");
        await _outputChannel.Writer.WriteAsync(batch, ct);

        // Re-initialize for next batch
        _builders = CreateBuilders(_schema);
        _appenders = CreateAppenders(_builders);
        _rowsInBuffer = 0;
    }

    private IArrowArray BuildArray(IArrowArrayBuilder builder)
    {
        return builder switch
        {
            BooleanArray.Builder b => b.Build(),
            Int32Array.Builder b => b.Build(),
            Int64Array.Builder b => b.Build(),
            DoubleArray.Builder b => b.Build(),
            FloatArray.Builder b => b.Build(),
            StringArray.Builder b => b.Build(),
            Date64Array.Builder b => b.Build(),
            TimestampArray.Builder b => b.Build(),
            BinaryArray.Builder b => b.Build(),
            _ => throw new NotSupportedException($"Unsupported builder type: {builder.GetType().Name}")
        };
    }

    private Schema BuildSchema(IReadOnlyList<PipeColumnInfo> columns)
    {
        var builder = new Schema.Builder();
        foreach (var col in columns)
        {
            builder.Field(new Field(col.Name, GetArrowType(col.ClrType), col.IsNullable));
        }
        return builder.Build();
    }

    private IArrowType GetArrowType(Type type)
    {
        var baseType = Nullable.GetUnderlyingType(type) ?? type;

        if (baseType == typeof(string)) return StringType.Default;
        if (baseType == typeof(bool)) return BooleanType.Default;
        if (baseType == typeof(int)) return Int32Type.Default;
        if (baseType == typeof(long)) return Int64Type.Default;
        if (baseType == typeof(float)) return FloatType.Default;
        if (baseType == typeof(double)) return DoubleType.Default;
        if (baseType == typeof(decimal)) return DoubleType.Default;
        if (baseType == typeof(DateTime)) return Date64Type.Default;
        if (baseType == typeof(DateTimeOffset)) return TimestampType.Default;
        if (baseType == typeof(byte[])) return BinaryType.Default;

        return StringType.Default;
    }

    private List<IArrowArrayBuilder> CreateBuilders(Schema schema)
    {
        var builders = new List<IArrowArrayBuilder>();
        foreach (var field in schema.FieldsList)
        {
            builders.Add(CreateBuilder(field.DataType));
        }
        return builders;
    }

    private IArrowArrayBuilder CreateBuilder(IArrowType type)
    {
        return type.TypeId switch
        {
            ArrowTypeId.Boolean => new BooleanArray.Builder(),
            ArrowTypeId.Int32 => new Int32Array.Builder(),
            ArrowTypeId.Int64 => new Int64Array.Builder(),
            ArrowTypeId.Double => new DoubleArray.Builder(),
            ArrowTypeId.Float => new FloatArray.Builder(),
            ArrowTypeId.String => new StringArray.Builder(),
            ArrowTypeId.Timestamp => new TimestampArray.Builder(),
            ArrowTypeId.Date64 => new Date64Array.Builder(),
            ArrowTypeId.Binary => new BinaryArray.Builder(),
            _ => new StringArray.Builder()
        };
    }

    private Action<object?>[] CreateAppenders(List<IArrowArrayBuilder> builders)
    {
        var appenders = new Action<object?>[builders.Count];
        for (int i = 0; i < builders.Count; i++)
        {
            appenders[i] = CreateAppender(builders[i]);
        }
        return appenders;
    }

    private Action<object?> CreateAppender(IArrowArrayBuilder builder)
    {
        return builder switch
        {
            BooleanArray.Builder b => val => { if (val is bool v) b.Append(v); else b.AppendNull(); },
            Int32Array.Builder b => val => {
                if (val is int v) b.Append(v);
                else if (val is long l) b.Append((int)l);
                else b.AppendNull();
            },
            Int64Array.Builder b => val => {
                if (val is long v) b.Append(v);
                else if (val is int i) b.Append(i);
                else b.AppendNull();
            },
            DoubleArray.Builder b => val => {
                if (val is double v) b.Append(v);
                else if (val is float f) b.Append(f);
                else if (val is decimal d) b.Append((double)d);
                else b.AppendNull();
            },
            FloatArray.Builder b => val => {
                if (val is float v) b.Append(v);
                else if (val is double d) b.Append((float)d);
                else b.AppendNull();
            },
            StringArray.Builder b => val => {
                if (val is null) b.AppendNull();
                else b.Append(val.ToString());
            },
            Date64Array.Builder b => val => {
                if (val is DateTime dt) b.Append(dt);
                else b.AppendNull();
            },
            TimestampArray.Builder b => val => {
                if (val is DateTimeOffset dto) b.Append(dto);
                else if (val is DateTime dt) b.Append(new DateTimeOffset(dt));
                else b.AppendNull();
            },
            BinaryArray.Builder b => val => {
                if (val is byte[] bytes) b.Append((System.Collections.Generic.IEnumerable<byte>)bytes);
                else b.AppendNull();
            },
            _ => val => {
                if (builder is StringArray.Builder s)
                {
                    if (val is null) s.AppendNull();
                    else s.Append(val.ToString());
                }
            }
        };
    }

    public async ValueTask DisposeAsync()
    {
        await CompleteAsync();
    }
}
