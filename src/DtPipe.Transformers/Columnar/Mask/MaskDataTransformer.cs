using System.Text;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Apache.Arrow;

namespace DtPipe.Transformers.Columnar.Mask;

/// <summary>
/// Masks specified columns using a pattern. # = keep original, any other char = replacement.
/// </summary>
public class MaskDataTransformer : IColumnarTransformer, IRequiresOptions<DtPipe.Transformers.Columnar.Mask.MaskOptions>
{
	private const char KeepChar = '#';

	private readonly Dictionary<string, string> _columnPatterns;
	private readonly bool _skipNull;
	private Dictionary<int, string>? _indexPatterns;

	public bool CanProcessColumnar => true;

	public MaskDataTransformer(DtPipe.Transformers.Columnar.Mask.MaskOptions options)
	{
		_columnPatterns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		_skipNull = options.SkipNull;

		foreach (var mapping in options.Mask)
		{
			var delimiterChars = new[] { ':', '=' };
			var delimiterIndex = mapping.IndexOfAny(delimiterChars);

			if (delimiterIndex > 0)
			{
				var column = mapping[..delimiterIndex].Trim();
				var pattern = mapping[(delimiterIndex + 1)..];
				_columnPatterns[column] = pattern;
			}
			else
			{
				// No delimiter? Assume entire string is column name and use default mask
				var column = mapping.Trim();
				// Default pattern: Mask with 15 asterisks (safe default)
				_columnPatterns[column] = "***************";
			}
		}
	}

	public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		if (_columnPatterns.Count == 0)
		{
			_indexPatterns = null;
			return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
		}

		_indexPatterns = new Dictionary<int, string>();

		for (var i = 0; i < columns.Count; i++)
		{
			if (_columnPatterns.TryGetValue(columns[i].Name, out var pattern))
			{
				_indexPatterns[i] = pattern;
			}
		}

		if (_indexPatterns.Count == 0)
		{
			_indexPatterns = null;
		}

		return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
	}

	public object?[]? Transform(object?[] row)
	{
		if (_indexPatterns == null)
		{
			return row;
		}

		foreach (var (idx, pattern) in _indexPatterns)
		{
			var value = row[idx];

			// Skip if source is null and SkipNull is enabled
			if (_skipNull && value is null)
			{
				continue;
			}

			if (value is string str)
			{
				row[idx] = ApplyMask(str, pattern);
			}
			else if (value != null)
			{
				// Convert to string, mask, keep as string
				row[idx] = ApplyMask(value.ToString() ?? "", pattern);
			}
		}

		return row;
	}

	public ValueTask<RecordBatch?> TransformBatchAsync(RecordBatch batch, CancellationToken ct = default)
	{
		if (_indexPatterns == null) return new ValueTask<RecordBatch?>(batch);

		var arrays = new IArrowArray[batch.Schema.FieldsList.Count];
		for (int i = 0; i < arrays.Length; i++)
		{
			if (_indexPatterns.TryGetValue(i, out var pattern))
			{
				var originalArray = batch.Column(i);
				arrays[i] = MaskArray(originalArray, pattern, batch.Length);
			}
			else
			{
				arrays[i] = batch.Column(i);
			}
		}

		return new ValueTask<RecordBatch?>(new RecordBatch(batch.Schema, arrays, batch.Length));
	}

	private IArrowArray MaskArray(IArrowArray original, string pattern, int length)
	{
		var builder = new StringArray.Builder();

		for (int i = 0; i < length; i++)
		{
			var val = GetStringFromArrow(original, i);
			if (val == null)
			{
				builder.AppendNull();
			}
			else
			{
				builder.Append(ApplyMask(val, pattern));
			}
		}

		return builder.Build();
	}

	private string? GetStringFromArrow(IArrowArray array, int index)
	{
		if (array.IsNull(index)) return null;

		return array switch
		{
			StringArray s => s.GetString(index),
			_ => GetValueFromArrow(array, index)?.ToString()
		};
	}

	private static object? GetValueFromArrow(IArrowArray array, int index)
	{
		if (array.IsNull(index)) return null;

		return array switch
		{
			StringArray a => a.GetString(index),
			Int32Array a => a.GetValue(index),
			Int64Array a => a.GetValue(index),
			DoubleArray a => a.GetValue(index),
			FloatArray a => a.GetValue(index),
			BooleanArray a => a.GetValue(index),
			Date64Array a => a.GetDateTime(index),
			TimestampArray a => a.GetTimestamp(index),
			_ => null
		};
	}

	private static string ApplyMask(string input, string pattern)
	{
		if (string.IsNullOrEmpty(input) || string.IsNullOrEmpty(pattern))
		{
			return input;
		}

		var sb = new StringBuilder(input.Length);

		for (var i = 0; i < input.Length; i++)
		{
			if (i < pattern.Length)
			{
				// Pattern character available: apply mask logic
				sb.Append(pattern[i] == KeepChar ? input[i] : pattern[i]);
			}
			else
			{
				// Pattern exhausted: keep remaining characters unmasked
				sb.Append(input[i]);
			}
		}

		return sb.ToString();
	}
}
