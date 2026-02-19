namespace DtPipe.Core.Abstractions;

/// <summary>
/// A transformer that can map a single input row to multiple output rows (1-to-N).
/// Used for expansion/unwinding operations.
/// </summary>
public interface IMultiRowTransformer : IDataTransformer
{
	/// <summary>
	/// Transforms a single row into multiple rows.
	/// </summary>
	/// <param name="row">The input row.</param>
	/// <returns>An enumerable of output rows.</returns>
	IEnumerable<object?[]> TransformMany(object?[] row);
}
