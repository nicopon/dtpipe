using DtPipe.Core.Models;
using DtPipe.Core.Services;
using DtPipe.Transformers.Expand;
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
}
