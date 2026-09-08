using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
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
	private string[] _created = [];

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

	public override async ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		await base.InitializeAsync(columns, ct);
		if (!HasOverwrite)
		{
			_columnValues = null;
			return columns;
		}

		// A mapping the incoming rows do not carry creates its column, appended after the real
		// ones — the rule 'fake' and 'format' already follow. Skipping it instead made a
		// misspelled name do nothing at all, which nothing in the output could show.
		_created = _staticMappings.Keys
			.Where(name => !columns.Any(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase)))
			.ToArray();

		var values = new string?[columns.Count + _created.Length];
		bool hasMappingForColumns = _created.Length > 0;

		for (var i = 0; i < columns.Count; i++)
		{
			if (_staticMappings.TryGetValue(columns[i].Name, out var val))
			{
				values[i] = val;
				hasMappingForColumns = true;
			}
		}

		for (var i = 0; i < _created.Length; i++)
			values[columns.Count + i] = _staticMappings[_created[i]];

		if (!hasMappingForColumns)
		{
			_columnValues = null;
			return columns;
		}

		_columnValues = values;

		var outputColumns = new List<PipeColumnInfo>(values.Length);
		for (var i = 0; i < columns.Count; i++)
		{
			outputColumns.Add(values[i] != null
				? columns[i] with { ClrType = typeof(string) }
				: columns[i]);
		}

		foreach (var name in _created)
			outputColumns.Add(new PipeColumnInfo(name, typeof(string), true));

		return outputColumns;
	}

	protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
	{
		if (_columnValues == null) return new ValueTask<RecordBatch?>(batch);

		var real = batch.Schema.FieldsList.Count;
		var arrays = new IArrowArray[real + _created.Length];
		var outputFields = new List<Field>(arrays.Length);

		for (int i = 0; i < real; i++)
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
				// Aliased column reused in the output batch — retain so the segment runner can
				// dispose the input without freeing buffers this output still points at.
				arrays[i] = ArrowOwnership.RetainArray(batch.Column(i));
				outputFields.Add(field);
			}
		}

		// A created column has no source value, so 'skip-null' has nothing to skip.
		for (int i = 0; i < _created.Length; i++)
		{
			arrays[real + i] = BuildConstant(_columnValues[real + i]!.ToString() ?? "", batch.Length);
			outputFields.Add(new Field(_created[i], StringType.Default, true));
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

	private static IArrowArray BuildConstant(string value, int length)
	{
		var builder = new StringArray.Builder();
		for (int i = 0; i < length; i++) builder.Append(value);
		return builder.Build();
	}
}
