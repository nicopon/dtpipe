using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace DtPipe.Core.Infrastructure.Arrow;

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

    public Schema? Schema => _schema;

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

    public void Fault(Exception exception)
    {
        _outputChannel.Writer.TryComplete(exception);
        _isComplete = true;
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
            arrays.Add(BuildArray(_builders[i]));
        }

        var batch = new RecordBatch(_schema, arrays, _rowsInBuffer);

        if (_isComplete)
        {
            // If already completing, don't wait for a slow reader that might have stopped
            _outputChannel.Writer.TryWrite(batch);
        }
        else
        {
            await _outputChannel.Writer.WriteAsync(batch, ct);
        }

        // Re-initialize for next batch
        _builders = CreateBuilders(_schema);
        _appenders = CreateAppenders(_builders);
        _rowsInBuffer = 0;
    }

    private IArrowArray BuildArray(IArrowArrayBuilder builder) => ArrowTypeMapper.BuildArray(builder);

    private Schema BuildSchema(IReadOnlyList<PipeColumnInfo> columns)
    {
        var builder = new Schema.Builder();
        foreach (var col in columns)
        {
            builder.Field(new Field(col.Name, ArrowTypeMapper.GetArrowType(col.ClrType), col.IsNullable));
        }
        return builder.Build();
    }

    private IArrowType GetArrowType(Type type) => ArrowTypeMapper.GetArrowType(type);

    private List<IArrowArrayBuilder> CreateBuilders(Schema schema)
    {
        var builders = new List<IArrowArrayBuilder>();
        foreach (var field in schema.FieldsList)
        {
            builders.Add(CreateBuilder(field.DataType));
        }
        return builders;
    }

    private IArrowArrayBuilder CreateBuilder(IArrowType type) => ArrowTypeMapper.CreateBuilder(type);

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
        return val => ArrowTypeMapper.AppendValue(builder, val);
    }

    public async ValueTask DisposeAsync()
    {
        await CompleteAsync();
    }
}
