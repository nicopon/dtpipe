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
            // Case-sensitive lookup first; fall back to case-insensitive (DuckDB EXPLAIN
            // returns lowercase names for unquoted identifiers in the query).
            var field = _underlying.Schema.GetFieldByName(name)
                ?? _underlying.Schema.FieldsList.FirstOrDefault(
                    f => string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase));

            fields.Add(field ?? new Field(name, StringType.Default, true));
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
            // Primary lookup: case-sensitive (fast path, column names match exactly).
            var index = _underlying.Schema.GetFieldIndex(name);

            // Fallback: case-insensitive. DuckDB EXPLAIN normalises unquoted identifiers to
            // lowercase, so "Id" in the Arrow schema may appear as "id" in the projection list.
            if (index < 0)
            {
                for (int fi = 0; fi < _underlying.Schema.FieldsList.Count; fi++)
                {
                    if (string.Equals(_underlying.Schema.FieldsList[fi].Name, name, StringComparison.OrdinalIgnoreCase))
                    {
                        index = fi;
                        break;
                    }
                }
            }

            if (index >= 0)
            {
                columns.Add(batch.Column(index));
            }
            else
            {
                // Column is not in the Arrow source schema at all (e.g. a DuckDB-internal
                // column injected by a specific query plan). Return a null array of the
                // correct length — a zero-length array would cause DuckDB to raise
                // "arrow_scan: array length mismatch".
                var nullArray = new Apache.Arrow.StringArray.Builder()
                    .AppendRange(Enumerable.Repeat<string?>(null, batch.Length))
                    .Build();
                columns.Add(nullArray);
            }
        }

        return new RecordBatch(_projectedSchema, columns, batch.Length);
    }

    public void Dispose()
    {
        _underlying.Dispose();
    }
}
