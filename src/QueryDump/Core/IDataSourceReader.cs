namespace QueryDump.Core;

/// <summary>
/// Information about a database column.
/// </summary>
public sealed record ColumnInfo(string Name, Type ClrType, bool IsNullable);

/// <summary>
/// Abstraction for reading data from a database source.
/// </summary>
public interface IDataSourceReader : IAsyncDisposable
{
    /// <summary>
    /// Gets the columns of the result set. Available after OpenAsync.
    /// </summary>
    IReadOnlyList<ColumnInfo>? Columns { get; }

    /// <summary>
    /// Opens the connection and executes the query.
    /// </summary>
    Task OpenAsync(CancellationToken ct = default);

    /// <summary>
    /// Reads rows in batches.
    /// </summary>
    IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, CancellationToken ct = default);
}
