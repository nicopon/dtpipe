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
	/// </summary>
	IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, CancellationToken ct = default);
}
