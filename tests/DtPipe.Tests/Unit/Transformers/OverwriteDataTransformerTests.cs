using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Overwrite;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests;

public class OverwriteDataTransformerTests
{
	[Fact]
	public async Task Transform_ShouldOverwriteColumn_WhenMappingExists()
	{
		var options = new OverwriteOptions { Overwrite = new[] { "CITY:Paris" } };
		var transformer = new OverwriteDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("CITY", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { "London" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().Be("Paris");
	}

	[Fact]
	public async Task Transform_ShouldIgnore_WhenColumnDoesNotExist()
	{
		var options = new OverwriteOptions { Overwrite = new[] { "UNKNOWN:Value" } };
		var transformer = new OverwriteDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("CITY", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { "London" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().Be("London");
	}

	[Fact]
	public async Task Transform_ShouldSkipOverwrite_WhenSkipNullEnabled_AndValueIsNull()
	{
		var options = new OverwriteOptions { Overwrite = new[] { "CITY:Paris" }, SkipNull = true };
		var transformer = new OverwriteDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("CITY", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns,
			new object?[] { null },
			new object?[] { "London" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().BeNull("Should not overwrite null because SkipNull is true");
		TestBatchBuilder.GetVal(result!, 0, 1).Should().Be("Paris", "Should still overwrite non-null values");
	}
}
