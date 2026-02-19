namespace DtPipe.Core.Abstractions;

using DtPipe.Core.Models;

/// <summary>
/// Interface for data writers
/// </summary>
public interface IDataWriter : IAsyncDisposable
{
	/// <summary>
	/// Initialize the writer with column metadata.
	/// </summary>
	ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default);

	/// <summary>
	/// Write a batch of rows.
	/// </summary>
	ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default);

	/// <summary>
	/// Complete writing and flush all buffers.
	/// </summary>
	ValueTask CompleteAsync(CancellationToken ct = default);

	/// <summary>
	/// Execute a raw command (e.g. SQL) against the target.
	/// </summary>
	ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default);
}
