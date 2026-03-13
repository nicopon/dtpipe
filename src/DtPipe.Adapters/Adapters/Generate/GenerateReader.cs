using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Generate;

public partial class GenerateReader : IStreamReader, IColumnarStreamReader, IRequiresOptions<GenerateReaderOptions>
{
	private readonly GenerateReaderOptions _options;
	private Schema? _arrowSchema;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
	public Schema? Schema => _arrowSchema;

	public GenerateReader(string config, string query, GenerateReaderOptions options)
	{
		_options = options;
		var countStr = config.StartsWith("generate:", StringComparison.OrdinalIgnoreCase)
			? config["generate:".Length..]
			: config;

		var parts = countStr.Split(new[] { ';', ',' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
		foreach (var part in parts)
		{
			if (part.StartsWith("count=", StringComparison.OrdinalIgnoreCase))
			{
				if (TryParseWithSuffix(part["count=".Length..], out var count))
					_options.RowCount = count;
			}
			else if (part.StartsWith("rate=", StringComparison.OrdinalIgnoreCase))
			{
				if (TryParseWithSuffix(part["rate=".Length..], out var rate))
					_options.RowsPerSecond = (int)rate;
			}
			else if (TryParseWithSuffix(part, out var rawCount))
			{
				_options.RowCount = rawCount;
			}
		}
	}

	private static bool TryParseWithSuffix(string input, out long result)
	{
		result = 0;
		if (string.IsNullOrWhiteSpace(input)) return false;

		input = input.Trim().ToLowerInvariant();
		long multiplier = 1;

		if (input.EndsWith("k"))
		{
			multiplier = 1000;
			input = input[..^1];
		}
		else if (input.EndsWith("m"))
		{
			multiplier = 1000000;
			input = input[..^1];
		}

		if (double.TryParse(input, out var val))
		{
			result = (long)(val * multiplier);
			return true;
		}

		return false;
	}

	public Task OpenAsync(CancellationToken ct = default)
	{
		Columns = new List<PipeColumnInfo>
		{
			new("GenerateIndex", typeof(long), false)
		};
		_arrowSchema = new Schema.Builder()
			.Field(f => f.Name("GenerateIndex").DataType(Int64Type.Default).Nullable(false))
			.Build();
		return Task.CompletedTask;
	}

	public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([EnumeratorCancellation] CancellationToken ct = default)
	{
		int batchSize = _options.ArrowBatchSize;
		long totalRows = _options.RowCount;
		long produced = 0;
		int? rowsPerSecond = _options.RowsPerSecond;
		var sw = rowsPerSecond.HasValue ? System.Diagnostics.Stopwatch.StartNew() : null;

		while (produced < totalRows)
		{
			ct.ThrowIfCancellationRequested();

			int currentBatchLimit = (int)Math.Min(batchSize, totalRows - produced);
			var builder = new Int64Array.Builder();

			for (int i = 0; i < currentBatchLimit; i++)
			{
				builder.Append(produced + i);
			}

			var batch = new RecordBatch(_arrowSchema!, new IArrowArray[] { builder.Build() }, currentBatchLimit);
			yield return batch;

			produced += currentBatchLimit;
			await ThrottleAsync(produced, rowsPerSecond, sw, ct);
		}
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
