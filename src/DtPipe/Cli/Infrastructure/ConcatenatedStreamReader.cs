using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// Reads a list of files as one stream, opening each in turn.
/// </summary>
/// <remarks>
/// Built by <see cref="CliStreamReaderFactory"/> when the input is a local glob, so every file
/// adapter gains the behaviour without knowing about it.
///
/// The schema is the first file's, and a later file that does not match it fails the run naming
/// the file. Reading on with a mismatched schema would drop or misalign columns silently, which a
/// glob over a directory makes easy to do by accident — one stale file among fourteen.
/// </remarks>
public class ConcatenatedStreamReader : IStreamReader
{
	private readonly IReadOnlyList<string> _paths;
	private readonly Func<string, IStreamReader> _open;
	private IStreamReader? _current;
	private int _index = -1;

	public ConcatenatedStreamReader(IReadOnlyList<string> paths, Func<string, IStreamReader> open)
	{
		_paths = paths;
		_open = open;
	}

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

	protected IStreamReader Current => _current ?? throw new InvalidOperationException("Call OpenAsync first.");

	public async Task OpenAsync(CancellationToken ct = default)
	{
		await AdvanceAsync(ct);
		Columns = Current.Columns;
		OnFirstOpened(Current);
	}

	/// <summary>Hook for a subclass that needs the first reader's extra surface, such as its Arrow schema.</summary>
	protected virtual void OnFirstOpened(IStreamReader first) { }

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		while (true)
		{
			await foreach (var batch in Current.ReadBatchesAsync(batchSize, ct))
				yield return batch;

			if (!await MoveToNextAsync(ct)) yield break;
		}
	}

	/// <summary>
	/// Opens the next file, or returns false at the end of the list. The reader just drained is
	/// disposed before the next is opened, so a glob over many files holds one handle at a time.
	/// </summary>
	protected async Task<bool> MoveToNextAsync(CancellationToken ct)
	{
		if (_index + 1 >= _paths.Count) return false;

		var expected = Columns;
		await AdvanceAsync(ct);
		EnsureSchemaMatches(expected);
		return true;
	}

	private async Task AdvanceAsync(CancellationToken ct)
	{
		if (_current is not null) await _current.DisposeAsync();

		_index++;
		_current = _open(_paths[_index]);
		await _current.OpenAsync(ct);
	}

	private void EnsureSchemaMatches(IReadOnlyList<PipeColumnInfo>? expected)
	{
		var actual = Current.Columns;
		if (expected is null || actual is null) return;

		var mismatch = actual.Count != expected.Count;
		for (var i = 0; !mismatch && i < expected.Count; i++)
			mismatch = !string.Equals(expected[i].Name, actual[i].Name, StringComparison.OrdinalIgnoreCase);

		if (mismatch)
			throw new InvalidOperationException(
				$"'{_paths[_index]}' has columns [{Describe(actual)}] but '{_paths[0]}' has [{Describe(expected)}]. " +
				"Every file a glob matches must carry the same columns, in the same order.");
	}

	private static string Describe(IReadOnlyList<PipeColumnInfo> columns)
		=> string.Join(", ", columns.Select(c => c.Name));

	public async ValueTask DisposeAsync()
	{
		if (_current is not null) await _current.DisposeAsync();
		_current = null;
		GC.SuppressFinalize(this);
	}
}

/// <summary>
/// The columnar form, selected when the adapter's own reader is columnar — substituting the row
/// form would pull a Parquet glob out of Arrow and back for nothing.
/// </summary>
public sealed class ConcatenatedColumnarStreamReader : ConcatenatedStreamReader, IColumnarStreamReader
{
	public ConcatenatedColumnarStreamReader(IReadOnlyList<string> paths, Func<string, IStreamReader> open)
		: base(paths, open)
	{
	}

	public Schema? Schema { get; private set; }

	protected override void OnFirstOpened(IStreamReader first)
		=> Schema = ((IColumnarStreamReader)first).Schema;

	public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		while (true)
		{
			await foreach (var batch in ((IColumnarStreamReader)Current).ReadRecordBatchesAsync(ct))
				yield return batch;

			if (!await MoveToNextAsync(ct)) yield break;
		}
	}
}
