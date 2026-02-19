using DtPipe.Core.Models;

namespace DtPipe.Core.Abstractions;

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
	ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default);

	/// <summary>
	/// Transforms a single row. Returns the transformed row (may be the same instance, mutated).
	/// Returns null to drop the row from the pipeline.
	/// For pure streaming, each row is processed independently.
	/// </summary>
	object?[]? Transform(object?[] row);

	/// <summary>
	/// Flushes any buffered rows at the end of the stream.
	/// Stateful transformers (like Window/Aggregate) should implement this.
	/// </summary>
	IEnumerable<object?[]> Flush() => Enumerable.Empty<object?[]>();
}
