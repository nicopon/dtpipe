namespace QueryDump.Core;

/// <summary>
/// Interface for data writers
/// </summary>
public interface IDataWriter : IAsyncDisposable
{
    /// <summary>
    /// Initialize the writer with column metadata.
    /// </summary>
    ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default);

    /// <summary>
    /// Write a batch of rows.
    /// </summary>
    ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default);

    /// <summary>
    /// Complete writing and flush all buffers.
    /// </summary>
    ValueTask CompleteAsync(CancellationToken ct = default);

    /// <summary>
    /// Current file size in bytes.
    /// </summary>
    long BytesWritten { get; }
}
