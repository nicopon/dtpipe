using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.Extensions.Logging;

namespace DtPipe.Processors.Sql;

/// <summary>
/// Wraps a ChannelReader as an IArrowArrayStream for streaming FFI bridging. DuckDB reads it through the C
/// Data exporter, which passes no token, so the branch's own is what reaches the wait: without it a run
/// that stops leaves DuckDB, and so the run, waiting on a channel nobody completes. A cancellation stays a
/// failure of the stream, since ending it here would let the query finish on part of its input.
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
            if (await _reader.WaitToReadAsync(_ct))
            {
                if (_reader.TryRead(out var batch))
                {
                    return batch;
                }
            }
            return null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "ChannelArrowStream Error: {Message}", ex.Message);
            throw;
        }
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
    /// <summary>What every exported column name starts with: it names this stream in a plan.</summary>
    string Prefix { get; }

    /// <summary>The schema of the data the stream carries, under its own column names.</summary>
    Schema SourceSchema { get; }

    void SetProjectedColumns(IReadOnlyList<string>? columns);
}

/// <summary>
/// A stream DuckDB scans through the C Data Interface, which hands the scan only the columns the plan
/// uses, in the plan's order. The exported schema names each column behind <see cref="Prefix"/>, so the
/// projection a plan lists for a scan names the stream it belongs to: two streams that carry a column of
/// the same name are told apart, and a stream is never handed a column another scan asked for.
/// A scan ends on the stream's end, so a second end means a second scan, which found nothing or part of the
/// rows: it fails the query rather than let it finish on them. EXPLAIN cannot name a scan that reads no
/// column, so this is what catches one.
/// </summary>
internal sealed class ProjectedArrowStream : IProjectableArrowStream
{
    private readonly IArrowArrayStream _underlying;
    private readonly Schema _exportedSchema;
    private List<string>? _projections;
    private Schema? _projectedSchema;
    private readonly string _alias;
    private int _ends;

    public ProjectedArrowStream(IArrowArrayStream underlying, string prefix, string alias)
    {
        _underlying = underlying;
        _alias = alias;
        Prefix = prefix;
        var source = underlying.Schema;
        var fields = source.FieldsList
            .Select(f => new Field(prefix + f.Name, f.DataType, f.IsNullable, f.HasMetadata ? f.Metadata : null))
            .ToList();
        _exportedSchema = new Schema(fields, source.HasMetadata ? source.Metadata : null);
    }

    public string Prefix { get; }

    public Schema SourceSchema => _underlying.Schema;

    public Schema Schema => _exportedSchema;

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
        if (batch == null)
        {
            if (Interlocked.Exchange(ref _ends, 1) != 0)
                throw new InvalidOperationException(
                    $"'{_alias}' was read to its end a second time: the query scans it more than once, and a source is a stream a scan reads to its end. Read '{_alias}' once in a CTE (WITH c AS MATERIALIZED (SELECT * FROM {_alias}) ...) and select from that.");
            return null;
        }

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
