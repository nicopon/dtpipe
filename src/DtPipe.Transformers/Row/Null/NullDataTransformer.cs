using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Transformers.Row.Null;

/// <summary>
/// Sets specified columns to null. Priority: 10 (First).
/// </summary>
public class NullDataTransformer : IColumnarTransformer, IRequiresOptions<DtPipe.Transformers.Row.Null.NullOptions>
{
	private readonly HashSet<string> _nullColumns;
	private int[]? _targetIndices;

	public NullDataTransformer(DtPipe.Transformers.Row.Null.NullOptions options)
	{
		_nullColumns = new HashSet<string>(options.Columns.Select(c => c.Trim()), StringComparer.OrdinalIgnoreCase);
	}

	public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		if (_nullColumns.Count == 0)
		{
			_targetIndices = null;
			return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
		}

		var indices = new List<int>();
		for (var i = 0; i < columns.Count; i++)
		{
			if (_nullColumns.Contains(columns[i].Name))
			{
				indices.Add(i);
			}
		}

		_targetIndices = indices.Count > 0 ? indices.ToArray() : null;
		return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
	}

	public object?[]? Transform(object?[] row)
	{
		if (_targetIndices == null)
		{
			return row;
		}

		foreach (var idx in _targetIndices)
		{
			row[idx] = null;
		}

		return row;
	}

	public ValueTask<RecordBatch?> TransformBatchAsync(RecordBatch batch, CancellationToken ct = default)
	{
		if (_targetIndices == null) return new ValueTask<RecordBatch?>(batch);

		var arrays = new IArrowArray[batch.Schema.FieldsList.Count];
		for (int i = 0; i < arrays.Length; i++)
		{
			arrays[i] = batch.Column(i);
		}

		foreach (var idx in _targetIndices)
		{
			var field = batch.Schema.FieldsList[idx];
			arrays[idx] = CreateNullArray(field.DataType, batch.Length);
		}

		return new ValueTask<RecordBatch?>(new RecordBatch(batch.Schema, arrays, batch.Length));
	}

	private static IArrowArray CreateNullArray(IArrowType type, int length)
	{
		if (type is StringType) { var b = new StringArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
		if (type is Int32Type) { var b = new Int32Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
		if (type is Int64Type) { var b = new Int64Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
		if (type is DoubleType) { var b = new DoubleArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
		if (type is FloatType) { var b = new FloatArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
		if (type is BooleanType) { var b = new BooleanArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
		if (type is Date64Type) { var b = new Date64Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
		if (type is TimestampType) { var b = new TimestampArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }

		// Fallback to string
		var fb = new StringArray.Builder();
		for (int i = 0; i < length; i++) fb.AppendNull();
		return fb.Build();
	}
}
