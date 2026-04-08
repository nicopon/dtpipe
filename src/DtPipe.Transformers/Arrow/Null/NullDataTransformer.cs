using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Transformers.Arrow.Null;

/// <summary>
/// Sets specified columns to null. Priority: 10 (First).
/// </summary>
public class NullDataTransformer : BaseColumnarTransformer, IRequiresOptions<DtPipe.Transformers.Arrow.Null.NullOptions>
{
	private readonly HashSet<string> _nullColumns;
	private int[]? _targetIndices;

	public override bool CanProcessColumnar => true;

	public NullDataTransformer(DtPipe.Transformers.Arrow.Null.NullOptions options)
	{
		_nullColumns = new HashSet<string>(options.Columns.Select(c => c.Trim()), StringComparer.OrdinalIgnoreCase);
	}

	public override ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
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

	protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
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
		var builder = ArrowTypeMapper.CreateBuilder(type);
		for (int i = 0; i < length; i++)
			ArrowTypeMapper.AppendNull(builder);
		return ArrowTypeMapper.BuildArray(builder);
	}
}
