using Apache.Arrow;
using DtPipe.Core.Models;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// A stream-level transformer that consumes one or more upstream Arrow channels and
/// produces a new RecordBatch stream. Unlike row/batch transformers, a stream transformer
/// replaces (or supplements) the entire input stream rather than processing it row by row.
///
/// Examples: SQL engine (DuckDB), stream merge (concatenate two channels).
/// </summary>
public interface IStreamTransformer : IAsyncDisposable
{
    /// <summary>
    /// Opens the transformer: materializes any reference channels, waits for upstream
    /// schemas, and prepares the execution context. Called before <see cref="ReadResultsAsync"/>.
    /// </summary>
    Task OpenAsync(CancellationToken ct = default);

    /// <summary>
    /// The output column schema. Available after <see cref="OpenAsync"/> completes.
    /// </summary>
    IReadOnlyList<PipeColumnInfo>? Columns { get; }

    /// <summary>
    /// The native Arrow schema. Available after <see cref="OpenAsync"/> completes.
    /// </summary>
    Apache.Arrow.Schema? Schema { get; }

    /// <summary>
    /// Streams the transformer's output as Arrow RecordBatches.
    /// May read directly from registered upstream channels (e.g. SQL engine)
    /// rather than consuming the <paramref name="inputStream"/>.
    /// </summary>
    /// <param name="inputStream">
    /// The primary input stream. Some transformers (e.g. <c>MergeTransformer</c>) consume
    /// this directly; others (e.g. <c>SqlTransformer</c>) ignore it and read channels internally.
    /// May be <c>null</c> when the transformer manages all input via channels.
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    IAsyncEnumerable<RecordBatch> ReadResultsAsync(
        IAsyncEnumerable<RecordBatch>? inputStream = null,
        CancellationToken ct = default);
}
