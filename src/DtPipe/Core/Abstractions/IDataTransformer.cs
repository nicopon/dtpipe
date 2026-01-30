namespace DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

/// <summary>
/// Interface for transforming data in the pipeline.
/// </summary>
public interface IDataTransformer
{
    /// <summary>
    /// Initializes the transformer with input column metadata.
    /// Returns the output column schema (may include additional virtual columns).
    /// This enables cascade initialization where output of one transformer
    /// becomes input to the next.
    /// </summary>
    ValueTask<IReadOnlyList<ColumnInfo>> InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default);

    /// <summary>
    /// Transforms a single row. Returns the transformed row (may be the same instance, mutated).
    /// For pure streaming, each row is processed independently.
    /// </summary>
    object?[] Transform(object?[] row);
}
