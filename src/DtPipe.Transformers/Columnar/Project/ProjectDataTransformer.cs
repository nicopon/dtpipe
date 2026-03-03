using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Transformers.Columnar.Project;
using Apache.Arrow;

namespace DtPipe.Transformers.Columnar.Project;

/// <summary>
/// Selects (Project) or Drops specific columns.
/// </summary>
public class ProjectDataTransformer : IColumnarTransformer, IRequiresOptions<DtPipe.Transformers.Columnar.Project.ProjectOptions>
{
	private readonly List<string>? _projectColumns; // Ordered list for projection
	private readonly HashSet<string>? _dropColumns; // Set for blacklist

	// Pre-calculated map: OutputIndex -> SourceIndex
	private int[]? _outputToSourceIndex;

	public bool CanProcessColumnar => true;

	public ProjectDataTransformer(DtPipe.Transformers.Columnar.Project.ProjectOptions options)
	{
		if (!string.IsNullOrWhiteSpace(options.Project))
		{
			_projectColumns = options.Project.Split(',', StringSplitOptions.RemoveEmptyEntries)
										   .Select(s => s.Trim())
										   .Where(s => !string.IsNullOrEmpty(s))
										   .ToList();
		}

		if (!string.IsNullOrWhiteSpace(options.Drop))
		{
			_dropColumns = new HashSet<string>(
				options.Drop.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim()),
				StringComparer.OrdinalIgnoreCase);
		}
	}

	public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		// If no options, pass through
		if (_projectColumns == null && _dropColumns == null)
		{
			_outputToSourceIndex = null;
			return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
		}

		var sourceIndices = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
		for (int i = 0; i < columns.Count; i++)
		{
			sourceIndices[columns[i].Name] = i;
		}

		var newColumns = new List<PipeColumnInfo>();
		var indexMap = new List<int>();

		// Logic:
		// 1. Projection (WhiteList) has priority for defining output structure/order.
		if (_projectColumns != null)
		{
			foreach (var colName in _projectColumns)
			{
				// Check blacklist collision (Drop takes precedence, even if explicitly projected)
				if (_dropColumns != null && _dropColumns.Contains(colName))
				{
					continue;
				}

				if (sourceIndices.TryGetValue(colName, out var srcIndex))
				{
					newColumns.Add(columns[srcIndex]);
					indexMap.Add(srcIndex);
				}
				// If requested column missing, we ignore it (per standard loose projection).
			}
		}
		else
		{
			// 2. No Projection -> Keep all source columns except Dropped
			for (int i = 0; i < columns.Count; i++)
			{
				var col = columns[i];
				if (_dropColumns != null && _dropColumns.Contains(col.Name))
				{
					continue;
				}

				newColumns.Add(col);
				indexMap.Add(i);
			}
		}

		_outputToSourceIndex = indexMap.ToArray();
		return new ValueTask<IReadOnlyList<PipeColumnInfo>>(newColumns);
	}

	public object?[]? Transform(object?[] row)
	{
		if (_outputToSourceIndex == null) return row;

		var newRow = new object?[_outputToSourceIndex.Length];

		for (int i = 0; i < _outputToSourceIndex.Length; i++)
		{
			var srcIndex = _outputToSourceIndex[i];

			if (srcIndex >= 0 && srcIndex < row.Length)
			{
				newRow[i] = row[srcIndex];
			}
			// else: leave null (safeguard against row size mismatch)
		}

		return newRow;
	}

	public ValueTask<RecordBatch?> TransformBatchAsync(RecordBatch batch, CancellationToken ct = default)
	{
		if (_outputToSourceIndex == null) return new ValueTask<RecordBatch?>(batch);

		var fields = new List<Field>();
		var arrays = new List<IArrowArray>();

		foreach (var srcIndex in _outputToSourceIndex)
		{
			fields.Add(batch.Schema.FieldsList[srcIndex]);
			arrays.Add(batch.Column(srcIndex));
		}

		var newSchema = new Schema(fields, batch.Schema.Metadata);
		var newBatch = new RecordBatch(newSchema, arrays, batch.Length);
		return new ValueTask<RecordBatch?>(newBatch);
	}
}
