using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Overwrite;

/// <summary>
/// Overwrites specified columns with static values. Priority: 20.
/// </summary>
public class OverwriteDataTransformer : IDataTransformer, IRequiresOptions<OverwriteOptions>
{
	private readonly Dictionary<string, string> _staticMappings = new(StringComparer.OrdinalIgnoreCase);
	private readonly bool _skipNull;
	private object?[]? _columnValues;

	public bool HasOverwrite => _staticMappings.Count > 0;

	public OverwriteDataTransformer(OverwriteOptions options)
	{
		_skipNull = options.SkipNull;
		foreach (var mapping in options.Overwrite)
		{
			var parts = mapping.Split(new[] { ':', '=' }, 2);
			if (parts.Length == 2)
			{
				_staticMappings[parts[0].Trim()] = parts[1];
			}
		}
	}

	public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		if (!HasOverwrite) // Use the new property
		{
			_columnValues = null;
			return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
		}

		bool hasMappingForColumns = false;
		var values = new string?[columns.Count];

		for (var i = 0; i < columns.Count; i++)
		{
			if (_staticMappings.TryGetValue(columns[i].Name, out var val))
			{
				values[i] = val;
				hasMappingForColumns = true;
			}
			else
			{
				values[i] = null;
			}
		}

		if (!hasMappingForColumns)
		{
			_columnValues = null;
			return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
		}

		_columnValues = values;
		return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
	}

	public object?[]? Transform(object?[] row)
	{
		if (_columnValues == null)
		{
			return row;
		}

		// In-place modification
		for (var i = 0; i < row.Length; i++)
		{
			if (_columnValues[i] != null)
			{
				// Skip if source is null and SkipNull is enabled
				if (_skipNull && row[i] is null)
				{
					continue;
				}
				row[i] = _columnValues[i];
			}
		}

		return row;
	}
}
