using System.Runtime.CompilerServices;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Generate;

public partial class GenerateReader : IStreamReader, IRequiresOptions<GenerateReaderOptions>
{
	private readonly GenerateReaderOptions _options;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

	public GenerateReader(string config, string query, GenerateReaderOptions options)
	{
		_options = options;
	}

	public Task OpenAsync(CancellationToken ct = default)
	{
		Columns = new List<PipeColumnInfo>
		{
			new("GenerateIndex", typeof(long), false)
		};
		return Task.CompletedTask;
	}

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[EnumeratorCancellation] CancellationToken ct = default)
	{
		await Task.Yield();
		long totalRows = _options.RowCount;
		long produced = 0;
		int? rowsPerSecond = _options.RowsPerSecond;
		var sw = rowsPerSecond.HasValue ? System.Diagnostics.Stopwatch.StartNew() : null;

		var buffer = new object?[batchSize][];
		int bufferIndex = 0;

		while (produced < totalRows)
		{
			ct.ThrowIfCancellationRequested();

			long remaining = totalRows - produced;
			int currentBatchLimit = (int)Math.Min(batchSize, remaining);

			for (int i = 0; i < currentBatchLimit; i++)
			{
				var row = GenerateRow(produced + i);
				buffer[bufferIndex++] = row;

				if (bufferIndex >= batchSize)
				{
					yield return new ReadOnlyMemory<object?[]>(buffer, 0, bufferIndex);

					produced += bufferIndex;
					await ThrottleAsync(produced, rowsPerSecond, sw, ct);

					buffer = new object?[batchSize][]; // New buffer for next batch
					bufferIndex = 0;
				}
			}

			// Yield partial buffer if any (last batch)
			if (bufferIndex > 0)
			{
				yield return new ReadOnlyMemory<object?[]>(buffer, 0, bufferIndex);
				produced += bufferIndex;
				await ThrottleAsync(produced, rowsPerSecond, sw, ct);
				bufferIndex = 0;
			}
		}
	}

	private async Task ThrottleAsync(long produced, int? rowsPerSecond, System.Diagnostics.Stopwatch? sw, CancellationToken ct)
	{
		if (sw != null && rowsPerSecond.HasValue && rowsPerSecond.Value > 0)
		{
			double expectedMs = (double)produced / rowsPerSecond.Value * 1000;
			double actualMs = sw.Elapsed.TotalMilliseconds;
			if (actualMs < expectedMs)
			{
				await Task.Delay((int)(expectedMs - actualMs), ct);
			}
		}
	}

	private object?[] GenerateRow(long rowIndex)
	{
		return new object?[] { rowIndex };
	}

	public ValueTask DisposeAsync()
	{
		return ValueTask.CompletedTask;
	}
}
