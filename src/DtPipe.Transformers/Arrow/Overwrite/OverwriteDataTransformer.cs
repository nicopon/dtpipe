using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Transformers.Arrow.Overwrite;

/// <summary>
/// Overwrites specified columns with static values. Priority: 20.
/// </summary>
public class OverwriteDataTransformer : BaseColumnarTransformer, IRequiresOptions<DtPipe.Transformers.Arrow.Overwrite.OverwriteOptions>
{
	private readonly Dictionary<string, string> _staticMappings = new(StringComparer.OrdinalIgnoreCase);
	private readonly bool _skipNull;
	private object?[]? _columnValues;

	public bool HasOverwrite => _staticMappings.Count > 0;

	public override bool CanProcessColumnar => true;

	public OverwriteDataTransformer(DtPipe.Transformers.Arrow.Overwrite.OverwriteOptions options)
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

	public override ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		if (!HasOverwrite)
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

		var outputColumns = new List<PipeColumnInfo>(columns.Count);
		for (var i = 0; i < columns.Count; i++)
		{
			if (values[i] != null)
			{
				outputColumns.Add(columns[i] with { ClrType = typeof(string) });
			}
			else
			{
				outputColumns.Add(columns[i]);
			}
		}

		return new ValueTask<IReadOnlyList<PipeColumnInfo>>(outputColumns);
	}

	protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
	{
		if (_columnValues == null) return new ValueTask<RecordBatch?>(batch);

		var arrays = new IArrowArray[batch.Schema.FieldsList.Count];
		var outputFields = new List<Field>();

		for (int i = 0; i < arrays.Length; i++)
		{
			var field = batch.Schema.FieldsList[i];
			var staticVal = _columnValues[i];

			if (staticVal != null)
			{
				var originalArray = batch.Column(i);
				arrays[i] = ApplyOverwrite(originalArray, staticVal, batch.Length);
				outputFields.Add(new Field(field.Name, StringType.Default, field.IsNullable));
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

	private IArrowArray ApplyOverwrite(IArrowArray original, object value, int length)
	{
		var builder = new StringArray.Builder();
		var staticStr = value.ToString() ?? "";

		for (int i = 0; i < length; i++)
		{
			if (_skipNull && original.IsNull(i))
			{
				builder.AppendNull();
			}
			else
			{
				builder.Append(staticStr);
			}
		}

		return builder.Build();
	}
}
