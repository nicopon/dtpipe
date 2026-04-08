using System.Text;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Apache.Arrow;

namespace DtPipe.Transformers.Arrow.Mask;

/// <summary>
/// Masks specified columns using a pattern. # = keep original, any other char = replacement.
/// </summary>
public class MaskDataTransformer : BaseColumnarTransformer, IRequiresOptions<DtPipe.Transformers.Arrow.Mask.MaskOptions>
{
	private const char KeepChar = '#';

	private readonly Dictionary<string, string> _columnPatterns;
	private readonly bool _skipNull;
	private Dictionary<int, string>? _indexPatterns;

	public override bool CanProcessColumnar => true;

	public MaskDataTransformer(DtPipe.Transformers.Arrow.Mask.MaskOptions options)
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

	public override ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		if (_columnPatterns.Count == 0)
		{
			_indexPatterns = null;
			return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
		}

		_indexPatterns = new Dictionary<int, string>();

		var outputColumns = new PipeColumnInfo[columns.Count];
		bool anyMasked = false;

		for (var i = 0; i < columns.Count; i++)
		{
			if (_columnPatterns.TryGetValue(columns[i].Name, out var pattern))
			{
				_indexPatterns[i] = pattern;
				// MaskArray always produces StringArray regardless of the original column type.
				outputColumns[i] = columns[i] with { ClrType = typeof(string) };
				anyMasked = true;
			}
			else
			{
				outputColumns[i] = columns[i];
			}
		}

		if (!anyMasked)
		{
			_indexPatterns = null;
			return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
		}

		return new ValueTask<IReadOnlyList<PipeColumnInfo>>(outputColumns);
	}

	protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
	{
		if (_indexPatterns == null) return new ValueTask<RecordBatch?>(batch);

		var arrays = new IArrowArray[batch.Schema.FieldsList.Count];
		var outputFields = new List<Field>(batch.Schema.FieldsList.Count);

		for (int i = 0; i < arrays.Length; i++)
		{
			var field = batch.Schema.FieldsList[i];
			if (_indexPatterns.TryGetValue(i, out var pattern))
			{
				arrays[i] = MaskArray(batch.Column(i), field, pattern, batch.Length);
				outputFields.Add(new Field(field.Name, Apache.Arrow.Types.StringType.Default, field.IsNullable));
			}
			else
			{
				arrays[i] = batch.Column(i);
				outputFields.Add(field);
			}
		}

		var newSchema = new Schema(outputFields, batch.Schema.Metadata);
		return new ValueTask<RecordBatch?>(new RecordBatch(newSchema, arrays, batch.Length));
	}

	private IArrowArray MaskArray(IArrowArray original, Field field, string pattern, int length)
	{
		var builder = new StringArray.Builder();

		for (int i = 0; i < length; i++)
		{
			if (original.IsNull(i))
			{
				builder.AppendNull();
			}
			else
			{
				var val = ArrowTypeMapper.GetValueForField(original, field, i)?.ToString() ?? "";
				builder.Append(ApplyMask(val, pattern));
			}
		}

		return builder.Build();
	}

	private static string ApplyMask(string input, string pattern)
	{
		if (string.IsNullOrEmpty(input) || string.IsNullOrEmpty(pattern))
		{
			return input;
		}

		return string.Create(input.Length, (input, pattern), (span, state) =>
		{
			var (inp, pat) = state;
			for (var i = 0; i < span.Length; i++)
			{
				if (i < pat.Length)
				{
					span[i] = pat[i] == KeepChar ? inp[i] : pat[i];
				}
				else
				{
					span[i] = inp[i];
				}
			}
		});
	}
}
