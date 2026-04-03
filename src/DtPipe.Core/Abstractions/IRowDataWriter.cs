using DtPipe.Core.Models;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Extension for writers that accept data in row-based format (object?[]).
/// </summary>
public interface IRowDataWriter : IDataWriter
{
    /// <summary>
    /// Write a batch of rows.
    /// </summary>
    ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default);
}
