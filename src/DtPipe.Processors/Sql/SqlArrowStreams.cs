using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.Extensions.Logging;

namespace DtPipe.Processors.Sql;

/// <summary>
/// Wraps a ChannelReader as an IArrowArrayStream for streaming FFI bridging.
/// </summary>
internal sealed class ChannelArrowStream : IArrowArrayStream
{
    private readonly Schema _schema;
    private readonly ChannelReader<RecordBatch> _reader;
    private readonly ILogger _logger;
    private readonly CancellationToken _ct;

    public ChannelArrowStream(Schema schema, ChannelReader<RecordBatch> reader, ILogger logger, CancellationToken ct)
    {
        _schema = schema;
        _reader = reader;
        _logger = logger;
        _ct = ct;
    }

    public Schema Schema => _schema;

    public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            if (await _reader.WaitToReadAsync(cancellationToken))
            {
                if (_reader.TryRead(out var batch))
                {
                    return batch;
                }
            }
            return null;
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ChannelArrowStream Error: {Message}", ex.Message);
            throw;
        }
        return null;
    }

    public void Dispose() { }
}

internal sealed class StaticArrowStream : IArrowArrayStream
{
    private readonly Schema _schema;
    private readonly IReadOnlyList<RecordBatch> _batches;
    private int _currentIndex = 0;

    public StaticArrowStream(Schema schema, IReadOnlyList<RecordBatch> batches)
    {
        _schema = schema;
        _batches = batches;
    }

    public Schema Schema => _schema;

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        if (_currentIndex < _batches.Count)
        {
            return new ValueTask<RecordBatch?>(_batches[_currentIndex++]);
        }
        return new ValueTask<RecordBatch?>(default(RecordBatch));
    }

    public void Dispose() { }
}

internal interface IProjectableArrowStream : IArrowArrayStream
{
    void SetProjectedColumns(IReadOnlyList<string>? columns);
}

internal sealed class ProjectedArrowStream : IProjectableArrowStream
{
    private readonly IArrowArrayStream _underlying;
    private List<string>? _projections;
    private Schema? _projectedSchema;

    public ProjectedArrowStream(IArrowArrayStream underlying)
    {
        _underlying = underlying;
    }

    public Schema Schema => _underlying.Schema;

    public void SetProjectedColumns(IReadOnlyList<string>? columns)
    {
        if (columns == null || columns.Count == 0)
        {
            _projections = null;
            _projectedSchema = null;
            return;
        }

        _projections = new List<string>(columns);
        var fields = new List<Field>();
        foreach (var name in _projections)
        {
            var field = _underlying.Schema.GetFieldByName(name);
            if (field != null)
            {
                fields.Add(field);
            }
            else
            {
                fields.Add(new Field(name, StringType.Default, true));
            }
        }
        _projectedSchema = new Schema(fields, _underlying.Schema.HasMetadata ? _underlying.Schema.Metadata : null);
    }

    public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        var batch = await _underlying.ReadNextRecordBatchAsync(cancellationToken);
        if (batch == null) return null;

        if (_projections == null || _projections.Count == 0 || _projectedSchema == null)
            return batch;

        var columns = new List<IArrowArray>();
        foreach (var name in _projections)
        {
            var index = _underlying.Schema.GetFieldIndex(name);
            if (index >= 0)
            {
                columns.Add(batch.Column(index));
            }
            else
            {
                columns.Add(new StringArray.Builder().Build());
            }
        }

        return new RecordBatch(_projectedSchema, columns, batch.Length);
    }

    public void Dispose()
    {
        _underlying.Dispose();
    }
}
