using DtPipe.Core.Models;
using DtPipe.Transformers.Services;
using DtPipe.Transformers.Row.Expand;
using Moq;
using Xunit;

namespace DtPipe.Tests.Transformers;

public class ExpandDataTransformerTests
{
	private readonly Mock<IJsEngineProvider> _jsProviderMock;
	private readonly JsEngineProvider _realJsProvider;

	public ExpandDataTransformerTests()
	{
		_jsProviderMock = new Mock<IJsEngineProvider>();
		_realJsProvider = new JsEngineProvider(); // Use real provider for tests to verify Jint logic
	}

	[Fact]
	public async Task InitializeAsync_ShouldNotModifyColumns()
	{
		// Arrange
		var options = new ExpandOptions { Expand = new[] { "return [row];" } };
		var transformer = new ExpandDataTransformer(options, _realJsProvider);
		var columns = new List<PipeColumnInfo> { new("Id", typeof(int), false), new("Name", typeof(string), true) };

		// Act
		var result = await transformer.InitializeAsync(columns);

		// Assert
		Assert.Equal(columns, result);
	}

	[Fact]
	public async Task TransformMany_ShouldExpandSingleRowIntoMultiple()
	{
		// Arrange
		// Script returns array of 2 objects
		var script = "return [ { Id: 1, Name: 'A' }, { Id: 2, Name: 'B' } ];";
		var options = new ExpandOptions { Expand = new[] { script } };
		var transformer = new ExpandDataTransformer(options, _realJsProvider);

		var columns = new List<PipeColumnInfo> { new("Id", typeof(int), false), new("Name", typeof(string), true) };
		await transformer.InitializeAsync(columns);

		var inputRow = new object?[] { 1, "Original" };

		// Act
		var results = transformer.TransformMany(inputRow).ToList();

		// Assert
		Assert.Equal(2, results.Count);

		Assert.Equal(1d, Convert.ToDouble(results[0][0])); // Jint numbers are doubles
		Assert.Equal("A", results[0][1]);

		Assert.Equal(2d, Convert.ToDouble(results[1][0]));
		Assert.Equal("B", results[1][1]);
	}

	[Fact]
	public async Task TransformMany_ShouldUseInputRowValues()
	{
		// Arrange
		var script = "return [ { Id: row.Id, Name: row.Name + '_1' }, { Id: row.Id, Name: row.Name + '_2' } ];";
		var options = new ExpandOptions { Expand = new[] { script } };
		var transformer = new ExpandDataTransformer(options, _realJsProvider);

		var columns = new List<PipeColumnInfo> { new("Id", typeof(int), false), new("Name", typeof(string), true) };
		await transformer.InitializeAsync(columns);

		var inputRow = new object?[] { 10, "Base" };

		// Act
		var results = transformer.TransformMany(inputRow).ToList();

		// Assert
		Assert.Equal(2, results.Count);
		Assert.Equal("Base_1", results[0][1]);
		Assert.Equal("Base_2", results[1][1]);
	}

	// ── A result of the wrong shape ─────────────────────────────────────

	/// <summary>
	/// An array of plain values produced no rows at all, silently and with exit code 0: the whole
	/// source disappeared into a zero-byte file. A nested JSON array also arrives as a wrapped CLR
	/// collection rather than a JS array, so the refusal has to be about the element, which is what
	/// a reader of the file actually sees.
	/// </summary>
	[Fact]
	public async Task TransformMany_AnArrayOfPlainValues_IsRefused()
	{
		var transformer = await Ready("return ['a', 'b'];");

		var ex = Assert.Throws<InvalidOperationException>(() => transformer.TransformMany(new object?[] { 1, "x" }).ToList());

		Assert.Contains("array of row objects", ex.Message);
		Assert.Contains("an element of the array is a string", ex.Message);
		Assert.Contains("--expand-types", ex.Message);
	}

	[Fact]
	public async Task TransformMany_AResultThatIsNotAnArray_IsRefused()
	{
		var transformer = await Ready("return 42;");

		var ex = Assert.Throws<InvalidOperationException>(() => transformer.TransformMany(new object?[] { 1, "x" }).ToList());

		Assert.Contains("returned a number", ex.Message);
	}

	/// <summary>
	/// The generated symbol ('__expand_80f14686…') and the Jint wrapper used to reach the user,
	/// who had no way to connect either to anything they had written.
	/// </summary>
	[Fact]
	public async Task TransformMany_AFailingExpression_Names_What_The_User_Wrote()
	{
		var transformer = await Ready("tags");

		var ex = Assert.Throws<InvalidOperationException>(() => transformer.TransformMany(new object?[] { 1, "x" }).ToList());

		Assert.Contains("'tags'", ex.Message);
		Assert.DoesNotContain("__expand_", ex.Message);
		Assert.Contains("row.<column>", ex.Message);
	}

	// ── A column the expression creates ─────────────────────────────────

	/// <summary>
	/// InitializeAsync fixes the schema before the first row, so a key the expression invents has
	/// to be declared. Nothing fed ExpandTypes before — no flag, no YAML key — which is why the
	/// shipped example produced every column except the one it was written to show.
	/// </summary>
	[Fact]
	public async Task A_Declared_Column_Is_Appended_To_The_Schema_And_Filled()
	{
		var options = new ExpandOptions
		{
			Expand = new[] { "row.Tags.map(t => ({ ...row, Tag: t }))" },
			ExpandTypes = new(StringComparer.OrdinalIgnoreCase) { ["Tag"] = "string" }
		};
		var transformer = new ExpandDataTransformer(options, _realJsProvider);
		var columns = new List<PipeColumnInfo> { new("Id", typeof(int), false), new("Tags", typeof(object), true) };

		var schema = await transformer.InitializeAsync(columns);

		Assert.Equal(new[] { "Id", "Tags", "Tag" }, schema.Select(c => c.Name));
		Assert.Equal(typeof(string), schema[2].ClrType);

		var results = transformer.TransformMany(new object?[] { 7, new[] { "a", "b" } }).ToList();

		Assert.Equal(2, results.Count);
		Assert.Equal("a", results[0][2]);
		Assert.Equal("b", results[1][2]);
		Assert.Equal(7, Convert.ToInt32(results[0][0]));
	}

	/// <summary>A declaration naming an existing column retypes it rather than adding a second one.</summary>
	[Fact]
	public async Task A_Declaration_Naming_An_Existing_Column_Retypes_It()
	{
		var options = new ExpandOptions
		{
			Expand = new[] { "return [row];" },
			ExpandTypes = new(StringComparer.OrdinalIgnoreCase) { ["Id"] = "long" }
		};
		var transformer = new ExpandDataTransformer(options, _realJsProvider);

		var schema = await transformer.InitializeAsync(
			new List<PipeColumnInfo> { new("Id", typeof(int), false), new("Name", typeof(string), true) });

		Assert.Equal(2, schema.Count);
		Assert.Equal(typeof(long), schema[0].ClrType);
	}

	/// <summary>A name alone declares a string column — the common case needs no type.</summary>
	[Fact]
	public async Task A_Declaration_Without_A_Type_Is_A_String_Column()
	{
		var options = new ExpandOptions
		{
			Expand = new[] { "return [row];" },
			ExpandTypes = new(StringComparer.OrdinalIgnoreCase) { ["Tag"] = "" }
		};
		var transformer = new ExpandDataTransformer(options, _realJsProvider);

		var schema = await transformer.InitializeAsync(new List<PipeColumnInfo> { new("Id", typeof(int), false) });

		Assert.Equal(typeof(string), schema[1].ClrType);
	}

	/// <summary>An unresolved hint was dropped, leaving the column silently at its source type.</summary>
	[Fact]
	public async Task A_Declaration_With_An_Unknown_Type_Is_Refused()
	{
		var options = new ExpandOptions
		{
			Expand = new[] { "return [row];" },
			ExpandTypes = new(StringComparer.OrdinalIgnoreCase) { ["Tag"] = "banana" }
		};
		var transformer = new ExpandDataTransformer(options, _realJsProvider);

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
			await transformer.InitializeAsync(new List<PipeColumnInfo> { new("Id", typeof(int), false) }));

		Assert.Contains("'banana'", ex.Message);
		Assert.Contains("names no type", ex.Message);
		Assert.Contains("int32", ex.Message);
	}

	/// <summary>
	/// The vocabulary is the one `--column-types` uses. `int32` resolved there and named nothing
	/// here, two flags apart, and the difference was invisible: the column simply stayed a string.
	/// </summary>
	[Theory]
	[InlineData("int32", typeof(int))]
	[InlineData("int64", typeof(long))]
	[InlineData("text", typeof(string))]
	[InlineData("timestamp", typeof(DateTimeOffset))]
	public async Task A_Declaration_Accepts_The_Same_Spellings_As_Column_Types(string hint, Type expected)
	{
		var options = new ExpandOptions
		{
			Expand = new[] { "return [row];" },
			ExpandTypes = new(StringComparer.OrdinalIgnoreCase) { ["Tag"] = hint }
		};
		var transformer = new ExpandDataTransformer(options, _realJsProvider);

		var schema = await transformer.InitializeAsync(new List<PipeColumnInfo> { new("Id", typeof(int), false) });

		Assert.Equal(expected, schema[1].ClrType);
	}

	private async Task<ExpandDataTransformer> Ready(string script)
	{
		var transformer = new ExpandDataTransformer(
			new ExpandOptions { Expand = new[] { script } }, _realJsProvider);
		await transformer.InitializeAsync(
			new List<PipeColumnInfo> { new("Id", typeof(int), false), new("Name", typeof(string), true) });
		return transformer;
	}
}
