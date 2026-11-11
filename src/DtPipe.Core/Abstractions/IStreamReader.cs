namespace DtPipe.Core.Abstractions;

using DtPipe.Core.Models;

/// <summary>
/// Abstraction for reading data from a database source.
/// </summary>
public interface IStreamReader : IAsyncDisposable
{
    /// <summary>
    /// Gets the columns of the result set. Available after OpenAsync.
    /// </summary>
    IReadOnlyList<PipeColumnInfo>? Columns { get; }

    /// <summary>
    /// Opens the connection and executes the query.
    /// </summary>
    Task OpenAsync(CancellationToken ct = default);

    /// <summary>
    /// Reads rows in batches.
    /// <para>
    /// The array of a yielded batch belongs to the consumer from the moment it is yielded: the reader never
    /// writes to it again, so a consumer may hold it or hand it on as it is. A batch holds at most
    /// <paramref name="batchSize"/> rows.
    /// </para>
    /// </summary>
    IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, CancellationToken ct = default);
}
