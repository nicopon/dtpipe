using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Apache.Arrow;

namespace DtPipe.Transformers.Columnar.Project;

/// <summary>
/// Selects (Project) or Drops specific columns.
/// </summary>
public class ProjectDataTransformer : IColumnarTransformer, IRequiresOptions<ProjectOptions>
{
	private readonly List<string>? _projectColumns; // Ordered list for projection
	private readonly HashSet<string>? _dropColumns;  // Set for blacklist
	private readonly IReadOnlyList<string>? _rawRenames; // Raw "Old:New" strings — parsed in InitializeAsync
	private Dictionary<string, string>? _renameMap; // Old -> New (built in InitializeAsync)

	// Pre-calculated map: OutputIndex -> SourceIndex
	private int[]? _outputToSourceIndex;
	private string[]? _outputNames;

	public bool CanProcessColumnar => true;

	public ProjectDataTransformer(ProjectOptions options)
	{
		if (options.Project.Any())
		{
			_projectColumns = options.Project
										   .SelectMany(s => s.Split(',', StringSplitOptions.RemoveEmptyEntries))
										   .Select(s => s.Trim())
										   .Where(s => !string.IsNullOrEmpty(s))
										   .ToList();
		}

		if (options.Drop.Any())
		{
			_dropColumns = new HashSet<string>(
				options.Drop.SelectMany(s => s.Split(',', StringSplitOptions.RemoveEmptyEntries)).Select(s => s.Trim()),
				StringComparer.OrdinalIgnoreCase);
		}

		if (options.Rename.Any())
		{
			_rawRenames = options.Rename
				.SelectMany(s => s.Split(',', StringSplitOptions.RemoveEmptyEntries))
				.Select(s => s.Trim())
				.Where(s => !string.IsNullOrEmpty(s))
				.ToList();
		}
	}

	public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		// Build rename map from raw strings (validated here, not in constructor)
		if (_rawRenames != null)
		{
			_renameMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
			foreach (var r in _rawRenames)
			{
				var parts = r.Split(':', 2);
				if (parts.Length != 2 || string.IsNullOrWhiteSpace(parts[0]) || string.IsNullOrWhiteSpace(parts[1]))
					throw new InvalidOperationException($"Invalid rename format: '{r}'. Expected 'OldName:NewName'.");
				_renameMap[parts[0].Trim()] = parts[1].Trim();
			}
		}

		// If no options, pass through
		if (_projectColumns == null && _dropColumns == null && _renameMap == null)
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
        var names = new List<string>();

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
                    var col = columns[srcIndex];
                    if (_renameMap != null && _renameMap.TryGetValue(col.Name, out var newName))
                    {
                        col = col with { Name = newName };
                    }
					newColumns.Add(col);
					indexMap.Add(srcIndex);
                    names.Add(col.Name);
				}
                else
                {
                    throw new InvalidOperationException($"Projected column '{colName}' not found in source schema.");
                }
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

                if (_renameMap != null && _renameMap.TryGetValue(col.Name, out var newName))
                {
                    col = col with { Name = newName };
                }

				newColumns.Add(col);
				indexMap.Add(i);
                names.Add(col.Name);
			}

            // Verify renameMap columns all existed and no collisions in output schema
            if (_renameMap != null)
            {
                foreach (var oldName in _renameMap.Keys)
                {
                    if (!sourceIndices.ContainsKey(oldName))
                    {
                         throw new InvalidOperationException($"Rename source column '{oldName}' not found in source schema.");
                    }
                }
            }

            var duplicateCheck = names.GroupBy(n => n, StringComparer.OrdinalIgnoreCase)
                                     .Where(g => g.Count() > 1)
                                     .Select(g => g.Key)
                                     .FirstOrDefault();

            if (duplicateCheck != null)
            {
                throw new InvalidOperationException($"Output schema contains duplicate column name: '{duplicateCheck}'. Rename or drop the conflicting column.");
            }
		}

		_outputToSourceIndex = indexMap.ToArray();
        _outputNames = names.ToArray();
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

		for (int i = 0; i < _outputToSourceIndex.Length; i++)
		{
            var srcIndex = _outputToSourceIndex[i];
            var oldField = batch.Schema.FieldsList[srcIndex];
            var newField = oldField;
            if (_outputNames != null)
            {
               newField = new Field(_outputNames[i], oldField.DataType, oldField.IsNullable, oldField.Metadata);
            }
			fields.Add(newField);
			arrays.Add(batch.Column(srcIndex));
		}

		var newSchema = new Schema(fields, batch.Schema.Metadata);
		var newBatch = new RecordBatch(newSchema, arrays, batch.Length);
		return new ValueTask<RecordBatch?>(newBatch);
	}
}
