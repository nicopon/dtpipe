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
        _rowsInBuffer = 0;

        _logger.LogDebug("Arrow Bridge initialized with {Count} columns, batch size {BatchSize}", columns.Count, batchSize);
        return ValueTask.CompletedTask;
    }

    public async ValueTask IngestRowsAsync(ReadOnlyMemory<object?[]> rows, CancellationToken ct = default)
    {
        if (_builders == null || _schema == null)
            throw new InvalidOperationException("Call InitializeAsync first.");

        for (int i = 0; i < rows.Length; i++)
        {
            var row = rows.Span[i];
            for (int colIdx = 0; colIdx < row.Length; colIdx++)
            {
                AppendValue(_builders[colIdx], row[colIdx]);
            }
            _rowsInBuffer++;

            if (_rowsInBuffer >= _batchSize)
            {
                await FlushCurrentBatchAsync(ct);
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
        foreach (var b in _builders)
        {
            // Use dynamic or explicit casting to build
            arrays.Add(BuildArray(b));
        }

        var batch = new RecordBatch(_schema, arrays, _rowsInBuffer);
        await _outputChannel.Writer.WriteAsync(batch, ct);

        _builders = CreateBuilders(_schema);
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

    private void AppendValue(IArrowArrayBuilder builder, object? value)
    {
        if (value == null)
        {
            if (builder is BooleanArray.Builder boolB) boolB.AppendNull();
            else if (builder is Int32Array.Builder i32B) i32B.AppendNull();
            else if (builder is Int64Array.Builder i64B) i64B.AppendNull();
            else if (builder is DoubleArray.Builder dblB) dblB.AppendNull();
            else if (builder is FloatArray.Builder fltB) fltB.AppendNull();
            else if (builder is StringArray.Builder strB) strB.AppendNull();
            else if (builder is Date64Array.Builder d64B) d64B.AppendNull();
            else if (builder is TimestampArray.Builder tsB) tsB.AppendNull();
            else if (builder is BinaryArray.Builder binB) binB.AppendNull();
            return;
        }

        if (builder is StringArray.Builder strB2) strB2.Append(value.ToString() ?? "");
        else if (builder is Int64Array.Builder i64B2 && value is long l) i64B2.Append(l);
        else if (builder is Int32Array.Builder i32B2 && value is int i) i32B2.Append(i);
        else if (builder is DoubleArray.Builder dblB2 && value is double d) dblB2.Append(d);
        else if (builder is DoubleArray.Builder dblB3 && value is float f) dblB3.Append(f);
        else if (builder is DoubleArray.Builder dblB4 && value is decimal dec) dblB4.Append((double)dec);
        else if (builder is FloatArray.Builder fltB2 && value is float f2) fltB2.Append(f2);
        else if (builder is BooleanArray.Builder boolB2 && value is bool b) boolB2.Append(b);
        else if (builder is Date64Array.Builder d64B2 && value is DateTime dt) d64B2.Append(dt);
        else if (builder is TimestampArray.Builder tsB2 && value is DateTimeOffset dto) tsB2.Append(dto);
        else if (builder is BinaryArray.Builder binB2 && value is byte[] bytes) binB2.Append((IEnumerable<byte>)bytes);
        else
        {
            if (builder is StringArray.Builder strB3) strB3.Append(value.ToString() ?? "");
        }
    }

    public async ValueTask DisposeAsync()
    {
        await CompleteAsync();
    }
}
