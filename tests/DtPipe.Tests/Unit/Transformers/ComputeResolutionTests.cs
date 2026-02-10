using DtPipe.Core.Models;
using DtPipe.Core.Services;
using DtPipe.Transformers.Script;
using FluentAssertions;
using Jint.Runtime;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

public class ComputeResolutionTests : IDisposable
{
	private readonly IJsEngineProvider _jsEngineProvider;

	public ComputeResolutionTests()
	{
		_jsEngineProvider = new DtPipe.Core.Services.JsEngineProvider();
	}

	public void Dispose()
	{
		_jsEngineProvider?.Dispose();
	}

	[Fact]
	public async Task Script_ShouldInjectRowData()
	{
		// Arrange
		var options = new ComputeOptions
		{
			Compute = new List<string> { "AGE:return row.AGE * 2" }
		};
		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
		// ComputeDataTransformer does NOT implement IDisposable anymore (provider does)

		var columns = new List<PipeColumnInfo> { new("AGE", typeof(int), true) };
		await transformer.InitializeAsync(columns);

		// Act
		var row = new object?[] { 21 };
		var result = transformer.Transform(row);

		// Assert
		result![0].Should().Be(42.0); // Jint numbers are doubles
	}

	[Fact]
	public async Task Script_ShouldBlockClrAccess()
	{
		// Arrange
		var options = new ComputeOptions
		{
			// Attempt to use System.IO.File
			Compute = new List<string> { "HACK:var file = System.IO.File; return 'pwned';" }
		};

		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);

		var columns = new List<PipeColumnInfo> { new("HACK", typeof(string), true) };

		// Act & Assert
		// Initialize compiles the script and executes it immediately if it's registration script? 
		// No, ComputeDataTransformer executes row transformation on Transform.
		// Initialize compiles the script
		await transformer.InitializeAsync(columns);

		Action act = () => transformer.Transform(new object?[] { "test" });

		// Changed: ComputeDataTransformer now wraps execution errors in InvalidOperationException
		act.Should().Throw<InvalidOperationException>()
		   .WithInnerException<JavaScriptException>();
	}
}
