using Apache.Arrow;
using DtPipe.Core.Models;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Bridge that ingests row arrays and produces Arrow RecordBatches.
/// This allows decoupling row-to-column conversion from data writers.
/// </summary>
public interface IRowToColumnarBridge : IAsyncDisposable
{
    /// <summary>
    /// Initializes the bridge with the expected schema and batch size.
    /// </summary>
    ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, int batchSize, CancellationToken ct = default);

    /// <summary>
    /// Ingests a batch of rows.
    /// </summary>
    ValueTask IngestRowsAsync(ReadOnlyMemory<object?[]> rows, CancellationToken ct = default);

    /// <summary>
    /// Pulls the produced RecordBatches.
    /// </summary>
    IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(CancellationToken ct = default);

    /// <summary>
    /// Faults the stream, sending the exception to consumers.
    /// </summary>
    void Fault(Exception exception);

    /// <summary>
    /// Flushes any partial buffer and marks as complete.
    /// </summary>
    ValueTask CompleteAsync(CancellationToken ct = default);
}
