using DtPipe.Core.Models;
using DtPipe.Core.Services;
using DtPipe.Transformers.Filter;
using Xunit;

namespace DtPipe.Tests.Transformers;

public class FilterDataTransformerTests
{
	private readonly JsEngineProvider _realJsProvider;

	public FilterDataTransformerTests()
	{
		_realJsProvider = new JsEngineProvider();
	}

	[Fact]
	public async Task Transform_ShouldKeepRow_WhenConditionIsTrue()
	{
		// Arrange
		var options = new FilterTransformerOptions { Filters = new[] { "return row.Age > 18;" } };
		var transformer = new FilterDataTransformer(options, _realJsProvider);

		var columns = new List<PipeColumnInfo> { new("Name", typeof(string), true), new("Age", typeof(int), false) };
		await transformer.InitializeAsync(columns);

		var row = new object?[] { "John", 25 };

		// Act
		var result = transformer.Transform(row);

		// Assert
		Assert.NotNull(result);
		Assert.Equal(row, result);
	}

	[Fact]
	public async Task Transform_ShouldDropRow_WhenConditionIsFalse()
	{
		// Arrange
		var options = new FilterTransformerOptions { Filters = new[] { "return row.Age > 18;" } };
		var transformer = new FilterDataTransformer(options, _realJsProvider);

		var columns = new List<PipeColumnInfo> { new("Name", typeof(string), true), new("Age", typeof(int), false) };
		await transformer.InitializeAsync(columns);

		var row = new object?[] { "Kid", 10 };

		// Act
		var result = transformer.Transform(row);

		// Assert
		Assert.Null(result);
	}

	[Fact]
	public async Task Transform_ShouldApplyMultipleFilters()
	{
		// Arrange
		var options = new FilterTransformerOptions { Filters = new[] { "row.A > 0", "row.B < 100" } };
		var transformer = new FilterDataTransformer(options, _realJsProvider);

		var columns = new List<PipeColumnInfo> { new("A", typeof(int), false), new("B", typeof(int), false) };
		await transformer.InitializeAsync(columns);

		// Case 1: Both true
		Assert.NotNull(transformer.Transform(new object?[] { 10, 50 }));

		// Case 2: First false
		Assert.Null(transformer.Transform(new object?[] { -1, 50 }));

		// Case 3: Second false
		Assert.Null(transformer.Transform(new object?[] { 10, 200 }));
	}
}
