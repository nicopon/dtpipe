using System.Linq;
using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Overwrite;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

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

	/// <summary>
	/// A mapping the incoming rows do not carry creates its column — the rule 'fake' and 'format'
	/// already follow, and 'overwrite' was the one columnar transformer that produced a value and
	/// did not. Skipping instead made a misspelled name do nothing that anything could show, and
	/// left writing a constant into a new column to 'compute', which is row-mode and costs the
	/// pipeline its columnar path.
	/// </summary>
	[Fact]
	public async Task Transform_ShouldCreateColumn_WhenItIsNotInTheSource()
	{
		var options = new OverwriteOptions { Overwrite = new[] { "TAG:Value" } };
		var transformer = new OverwriteDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("CITY", typeof(string), true) };

		var schema = await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);

		// Appended after the real columns, as a string.
		schema.Select(c => c.Name).Should().Equal("CITY", "TAG");
		schema[1].ClrType.Should().Be(typeof(string));

		var batch = TestBatchBuilder.FromRows(columns, new object?[] { "London" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().Be("London");
		TestBatchBuilder.GetVal(result!, 1, 0).Should().Be("Value");
	}

	/// <summary>A created column has no source value, so there is no null for skip-null to skip.</summary>
	[Fact]
	public async Task A_Created_Column_Is_Filled_Even_Under_SkipNull()
	{
		var options = new OverwriteOptions { Overwrite = new[] { "TAG:Value" }, SkipNull = true };
		var transformer = new OverwriteDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("CITY", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { null }, new object?[] { "London" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 1, 0).Should().Be("Value");
		TestBatchBuilder.GetVal(result!, 1, 1).Should().Be("Value");
	}

	/// <summary>Creating and overwriting in one block, each in its place.</summary>
	[Fact]
	public async Task An_Existing_And_A_Created_Column_Coexist()
	{
		var options = new OverwriteOptions { Overwrite = new[] { "CITY:Paris", "TAG:Value" } };
		var transformer = new OverwriteDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("CITY", typeof(string), true) };

		var schema = await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		schema.Select(c => c.Name).Should().Equal("CITY", "TAG");

		var batch = TestBatchBuilder.FromRows(columns, new object?[] { "London" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().Be("Paris");
		TestBatchBuilder.GetVal(result!, 1, 0).Should().Be("Value");
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
