namespace QueryDump.Core;

/// <summary>
/// Interface for transforming data batches in the pipeline.
/// </summary>
public interface IDataTransformer
{
    /// <summary>
    /// Initializes the transformer with column metadata.
    /// This is called once before processing batches.
    /// </summary>
    ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default);

    /// <summary>
    /// Transforms a batch of rows.
    /// </summary>
    ValueTask<IReadOnlyList<object?[]>> TransformAsync(IReadOnlyList<object?[]> batch, CancellationToken ct = default);
}
