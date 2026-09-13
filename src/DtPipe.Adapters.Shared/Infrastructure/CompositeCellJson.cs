using System.Collections;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Core.Helpers;

/// <summary>
/// Renders a composite cell — a JSON object, an Arrow struct, a list, an array column — as JSON
/// text, for a target that has no composite type of its own.
///
/// A target that carries the shape natively must not call this: DuckDB keeps a STRUCT, Parquet
/// keeps a LIST, JSONL keeps the nesting. What is rendered is the value; the column the writer
/// declared is what decides, and it is never widened or narrowed here.
///
/// Every writer that lacked this went wrong in its own way on the same two rows of JSONL, and one
/// of the five did not fail at all: Oracle's driver accepted the <c>Dictionary</c> and wrote
/// <c>System.Collections.Generic.Dictionary`2[System.String,System.Object]</c> into the column. A
/// writer that hands a composite to its driver untouched is not safe by default.
/// </summary>
public static class CompositeCellJson
{
	/// <summary>
	/// True when the value is a composite no scalar column can hold. <c>string</c> and
	/// <c>byte[]</c> are enumerable and are deliberately excluded: rendering them would turn
	/// every text cell into an array of characters and every BLOB into an array of numbers.
	/// </summary>
	public static bool IsComposite(object? value)
		=> value is IDictionary || (value is IEnumerable && value is not string && value is not byte[]);

	/// <summary>
	/// JSON for one composite value, serialized against its runtime type — against the static
	/// <c>IEnumerable</c>, System.Text.Json emits an empty object.
	/// </summary>
	public static string Render(object value) => JsonSerializer.Serialize(value, value.GetType());

	/// <summary>Renders a composite, passes anything else through untouched.</summary>
	public static object? RenderIfComposite(object? value)
		=> IsComposite(value) ? Render(value!) : value;

	/// <summary>
	/// Wraps a per-column cell converter so a composite reaching a scalar column becomes JSON
	/// before the driver ever sees it. Built once per column, like the converter it wraps.
	/// </summary>
	public static Func<object?, object?> Wrap(Func<object?, object?> inner)
		=> v => IsComposite(v) ? Render(v!) : inner(v);

	/// <summary>
	/// True when a field holds a composite Arrow type.
	/// </summary>
	public static bool IsCompositeField(Field field)
		=> field.DataType is StructType or ListType or LargeListType or MapType;

	/// <summary>
	/// Returns a batch whose composite columns are StringArrays of JSON, with the schema restated
	/// to match. <paramref name="keepNative"/> exempts the fields the target can carry as they
	/// are — Parquet passes it to keep a real LIST.
	/// </summary>
	/// <remarks>
	/// Ownership, per <c>CLAUDE.md</c> › "RecordBatch ownership": the caller keeps ownership of
	/// <paramref name="batch"/> and still disposes it. A batch holding no composite comes back as
	/// the SAME reference, so the ordinary case pays one schema scan and no copy — which is why
	/// the result is a <see cref="RenderedBatch"/> rather than a bare batch: disposing that
	/// pass-through a second time would over-release buffers the caller is still using.
	/// </remarks>
	public static RenderedBatch RenderComposites(RecordBatch batch, Func<int, Field, bool>? keepNative = null)
	{
		var fields = batch.Schema.FieldsList;
		var rendered = false;
		for (var i = 0; i < fields.Count && !rendered; i++)
			rendered = IsCompositeField(fields[i]) && !(keepNative?.Invoke(i, fields[i]) ?? false);

		if (!rendered) return new RenderedBatch(batch, owned: false);

		var newFields = new List<Field>(fields.Count);
		var arrays = new IArrowArray[fields.Count];

		for (var i = 0; i < fields.Count; i++)
		{
			var field = fields[i];
			var column = batch.Column(i);

			if (!IsCompositeField(field) || (keepNative?.Invoke(i, field) ?? false))
			{
				newFields.Add(field);
				arrays[i] = ArrowOwnership.RetainArray(column);
				continue;
			}

			newFields.Add(new Field(field.Name, StringType.Default, field.IsNullable));
			arrays[i] = BuildJsonColumn(column, batch.Length);
		}

		return new RenderedBatch(new RecordBatch(new Schema(newFields, batch.Schema.Metadata), arrays, batch.Length), owned: true);
	}

	private static IArrowArray BuildJsonColumn(IArrowArray column, int length)
	{
		var builder = new StringArray.Builder();
		for (var row = 0; row < length; row++)
		{
			var value = ArrowTypeMapper.GetValue(column, row);
			if (value is null) builder.AppendNull();
			else builder.Append(Render(value));
		}
		return builder.Build();
	}
}

/// <summary>
/// A batch on its way to a writer, and whether this holder owns it. Disposing releases the batch
/// only when <see cref="CompositeCellJson.RenderComposites"/> built a new one; a pass-through is
/// still owned by the caller that supplied it.
/// </summary>
public readonly struct RenderedBatch : IDisposable
{
	private readonly bool _owned;

	internal RenderedBatch(RecordBatch batch, bool owned)
	{
		Batch = batch;
		_owned = owned;
	}

	public RecordBatch Batch { get; }

	public void Dispose()
	{
		if (_owned) Batch.Dispose();
	}
}
