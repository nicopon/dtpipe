using Apache.Arrow;
using Apache.Arrow.Types;
using AwesomeAssertions;
using DtPipe.Core.Helpers;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Writers;

/// <summary>
/// A composite cell reaching a target that has no composite type must arrive as JSON. Five of the
/// seven writers threw on it; the other two — Oracle and DuckDB — accepted it and wrote
/// <c>System.Collections.Generic.Dictionary`2[System.String,System.Object]</c> into the column,
/// which is why these cases assert the rendered text and not merely that nothing threw.
/// </summary>
public class CompositeCellJsonTests
{
	[Fact]
	public void A_dictionary_is_composite()
		=> CompositeCellJson.IsComposite(new Dictionary<string, object?> { ["a"] = 1 }).Should().BeTrue();

	[Fact]
	public void A_list_is_composite()
		=> CompositeCellJson.IsComposite(new List<object?> { 1, 2 }).Should().BeTrue();

	[Fact]
	public void A_string_is_not_composite()
		=> CompositeCellJson.IsComposite("abc").Should().BeFalse(
			"a string is enumerable; rendering it would write an array of characters");

	[Fact]
	public void A_byte_array_is_not_composite()
		=> CompositeCellJson.IsComposite(new byte[] { 1, 2, 3 }).Should().BeFalse(
			"a BLOB is enumerable; rendering it would write an array of numbers");

	[Fact]
	public void A_scalar_is_not_composite()
	{
		CompositeCellJson.IsComposite(42).Should().BeFalse();
		CompositeCellJson.IsComposite(null).Should().BeFalse();
	}

	[Fact]
	public void Render_emits_json_for_a_nested_object()
		=> CompositeCellJson.Render(new Dictionary<string, object?> { ["severity"] = "info" })
			.Should().Be("""{"severity":"info"}""");

	[Fact]
	public void Render_emits_json_for_a_list()
		=> CompositeCellJson.Render(new List<object?> { 10, 20, 30 }).Should().Be("[10,20,30]");

	[Fact]
	public void Wrap_renders_a_composite_and_never_calls_the_inner_converter()
	{
		var innerCalled = false;
		var wrapped = CompositeCellJson.Wrap(_ => { innerCalled = true; return "inner"; });

		wrapped(new Dictionary<string, object?> { ["k"] = 1 }).Should().Be("""{"k":1}""");
		innerCalled.Should().BeFalse();
	}

	[Fact]
	public void Wrap_passes_a_scalar_to_the_inner_converter()
		=> CompositeCellJson.Wrap(v => $"inner:{v}")(7).Should().Be("inner:7");

	[Fact]
	public void A_batch_with_no_composite_comes_back_as_the_same_reference()
	{
		using var batch = ScalarBatch();

		using var rendered = CompositeCellJson.RenderComposites(batch);

		rendered.Batch.Should().BeSameAs(batch, "an ordinary batch must not pay for a copy");
	}

	[Fact]
	public void A_struct_column_becomes_a_json_string_column()
	{
		using var batch = StructBatch();

		using var rendered = CompositeCellJson.RenderComposites(batch);

		rendered.Batch.Should().NotBeSameAs(batch);
		rendered.Batch.Schema.FieldsList[1].DataType.Should().BeOfType<StringType>(
			"the schema must state what the column now holds");
		var payload = (StringArray)rendered.Batch.Column(1);
		payload.GetString(0).Should().Be("""{"severity":"info"}""");
		payload.GetString(1).Should().Be("""{"severity":"warn"}""");
	}

	[Fact]
	public void A_kept_native_column_is_left_alone()
	{
		using var batch = StructBatch();

		using var rendered = CompositeCellJson.RenderComposites(batch, (index, _) => index == 1);

		rendered.Batch.Should().BeSameAs(batch, "nothing was left to render");
	}

	[Fact]
	public void A_rendered_batch_outlives_the_batch_it_was_built_from()
	{
		var pool = new TrackingMemoryPool();
		RenderedBatch rendered;

		using (var batch = StructBatch(pool))
		{
			rendered = CompositeCellJson.RenderComposites(batch);
		}

		// The id column is a retained view over a buffer the disposed input owned.
		var id = (Int64Array)rendered.Batch.Column(0);
		id.GetValue(0).Should().Be(1);

		rendered.Dispose();
		pool.ActiveAllocations.Should().Be(0, "every retained buffer must come back");
	}

	private static RecordBatch ScalarBatch()
	{
		var schema = new Schema(new[] { new Field("id", Int64Type.Default, true) }, null);
		var id = new Int64Array.Builder().Append(1).Append(2).Build();
		return new RecordBatch(schema, new IArrowArray[] { id }, 2);
	}

	private static RecordBatch StructBatch(Apache.Arrow.Memory.MemoryAllocator? allocator = null)
	{
		var severity = new Field("severity", StringType.Default, true);
		var payloadType = new StructType(new[] { severity });
		var schema = new Schema(
			new[] { new Field("id", Int64Type.Default, true), new Field("payload", payloadType, true) },
			null);

		var id = new Int64Array.Builder().Append(1).Append(2).Build(allocator);
		var severityValues = new StringArray.Builder().Append("info").Append("warn").Build(allocator);
		var validity = new ArrowBuffer.BitmapBuilder().Append(true).Append(true).Build(allocator);
		var payload = new StructArray(payloadType, 2, new IArrowArray[] { severityValues }, validity);

		return new RecordBatch(schema, new IArrowArray[] { id, payload }, 2);
	}
}
