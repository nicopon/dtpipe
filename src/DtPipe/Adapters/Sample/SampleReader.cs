using System.Runtime.CompilerServices;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Sample;

public partial class SampleReader : IStreamReader, IRequiresOptions<SampleReaderOptions>
{
	private readonly SampleReaderOptions _options;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

	public SampleReader(string config, string query, SampleReaderOptions options)
	{
		_options = options;
	}

	public Task OpenAsync(CancellationToken ct = default)
	{
		// Build Schema from options
		var cols = new List<PipeColumnInfo>();
		foreach (var def in _options.ColumnDefinitions)
		{
			cols.Add(new PipeColumnInfo(def.Name, def.Type, false));
		}
		Columns = cols;
		return Task.CompletedTask;
	}

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[EnumeratorCancellation] CancellationToken ct = default)
	{
		await Task.Yield();
		long totalRows = _options.RowCount;
		long produced = 0;

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
					buffer = new object?[batchSize][]; // New buffer for next batch
					bufferIndex = 0;
				}
			}
			produced += currentBatchLimit;

			// Yield partial buffer if any
			if (bufferIndex > 0)
			{
				yield return new ReadOnlyMemory<object?[]>(buffer, 0, bufferIndex);
				bufferIndex = 0;
			}
		}
	}

	private object?[] GenerateRow(long rowIndex)
	{
		var cols = _options.ColumnDefinitions;
		var row = new object?[cols.Count];

		for (int i = 0; i < cols.Count; i++)
		{
			var def = cols[i];
			if (def.Type == typeof(int))
			{
				row[i] = (int)(rowIndex % int.MaxValue);
			}
			else if (def.Type == typeof(long))
			{
				row[i] = rowIndex;
			}
			else if (def.Type == typeof(double))
			{
				row[i] = rowIndex * 1.1;
			}
			else if (def.Type == typeof(bool))
			{
				row[i] = rowIndex % 2 == 0;
			}
			else if (def.Type == typeof(DateTime))
			{
				row[i] = DateTime.Now;
			}
			else if (def.Type == typeof(Guid))
			{
				row[i] = Guid.NewGuid();
			}
			else
			{
				row[i] = $"{def.Name} {rowIndex}";
			}
		}
		return row;
	}

	public ValueTask DisposeAsync()
	{
		return ValueTask.CompletedTask;
	}
}
