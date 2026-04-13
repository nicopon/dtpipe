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
        _schema = ArrowFfiWorkaround.ReorderSchema(schema);
        _reader = reader;
        _logger = logger;
        _ct = ct;
    }

    public Schema Schema => _schema;

    public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_ct, cancellationToken);
            if (await _reader.WaitToReadAsync(linkedCts.Token).ConfigureAwait(false) && _reader.TryRead(out var batch))
            {
                return ArrowFfiWorkaround.ReorderBatch(batch, _schema);
            }
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

/// <summary>
/// Wraps a pre-drained list of RecordBatches as an IArrowArrayStream.
/// This is used for reference tables that need to be zero-copy while remaining repeatable
/// (by using a new stream instance for each scan).
/// </summary>
internal sealed class StaticArrowStream : IArrowArrayStream
{
    private readonly Schema _schema;
    private readonly IReadOnlyList<RecordBatch> _batches;
    private int _currentIndex = 0;

    public StaticArrowStream(Schema schema, IReadOnlyList<RecordBatch> batches)
    {
        _schema = ArrowFfiWorkaround.ReorderSchema(schema);
        _batches = batches;
    }

    public Schema Schema => _schema;

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        if (_currentIndex < _batches.Count)
        {
            return new ValueTask<RecordBatch?>(ArrowFfiWorkaround.ReorderBatch(_batches[_currentIndex++], _schema));
        }
        return new ValueTask<RecordBatch?>(default(RecordBatch));
    }

    public void Dispose() { }
}

/// <summary>
/// Workaround for a bug in Apache.Arrow C# 22.1.0 CArrowArrayExporter.ExportRecordBatch:
/// when a variable-length column (String/Binary, n_buffers=3) precedes a fixed-width column
/// (Numeric, n_buffers=2), the fixed-width column receives the offset buffer pointer of the
/// String column instead of its own data buffer pointer. This causes data corruption when
/// native engines (DataFusion, DuckDB) read multi-column batches via the C Data Interface.
///
/// Workaround: reorder schema fields so that variable-length columns (String, Binary) come
/// after fixed-width columns (Numeric, Struct, etc.). SQL queries that reference fields by
/// name are unaffected by column reordering.
/// </summary>
internal static class ArrowFfiWorkaround
{
    /// <summary>
    /// Returns a reordered schema where variable-length columns (String, Binary) come last.
    /// </summary>
    internal static Schema ReorderSchema(Schema schema)
    {
        if (!NeedsReordering(schema.FieldsList)) return schema;

        var fields = ReorderFields(schema.FieldsList);
        return new Schema(fields, schema.Metadata);
    }

    /// <summary>
    /// Returns a RecordBatch with columns reordered to match the given reordered schema.
    /// If no reordering is needed, the original batch is returned unchanged.
    /// </summary>
    internal static RecordBatch ReorderBatch(RecordBatch batch, Schema reorderedSchema)
    {
        if (ReferenceEquals(batch.Schema, reorderedSchema) || batch.Schema.FieldsList.Count != reorderedSchema.FieldsList.Count)
            return batch;

        // Check if the order actually changed
        bool sameOrder = true;
        for (int i = 0; i < reorderedSchema.FieldsList.Count; i++)
        {
            if (reorderedSchema.FieldsList[i].Name != batch.Schema.FieldsList[i].Name)
            {
                sameOrder = false;
                break;
            }
        }
        if (sameOrder) return batch;

        // Build new column list in reordered schema order
        var columns = new IArrowArray[reorderedSchema.FieldsList.Count];
        for (int i = 0; i < reorderedSchema.FieldsList.Count; i++)
        {
            var fieldName = reorderedSchema.FieldsList[i].Name;
            int originalIndex = batch.Schema.GetFieldIndex(fieldName);
            columns[i] = batch.Column(originalIndex);
        }
        return new RecordBatch(reorderedSchema, columns, batch.Length);
    }

    private static bool NeedsReordering(IReadOnlyList<Field> fields)
    {
        bool sawVariableLength = false;
        foreach (var f in fields)
        {
            if (IsVariableLength(f.DataType))
                sawVariableLength = true;
            else if (sawVariableLength)
                return true; // fixed-width after variable-length → needs reorder
        }
        return false;
    }

    private static List<Field> ReorderFields(IReadOnlyList<Field> fields)
    {
        var fixedWidth = new List<Field>();
        var variableLength = new List<Field>();
        foreach (var f in fields)
        {
            if (IsVariableLength(f.DataType))
                variableLength.Add(f);
            else
                fixedWidth.Add(f);
        }
        var result = new List<Field>(fields.Count);
        result.AddRange(fixedWidth);
        result.AddRange(variableLength);
        return result;
    }

    // Variable-length column types have n_buffers=3 (validity + offsets + data).
    // These are the types that trigger the CArrowArrayExporter bug when preceding
    // fixed-width types (n_buffers=2).
    private static bool IsVariableLength(IArrowType type) =>
        type is StringType or BinaryType or LargeStringType or LargeBinaryType;
}
