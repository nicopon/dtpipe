using DtPipe.Core.Models;
using DtPipe.Transformers.Project;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

public class ProjectDataTransformerTests
{
	private readonly IReadOnlyList<PipeColumnInfo> _sourceSchema = new List<PipeColumnInfo>
	{
		new("A", typeof(int), false),
		new("B", typeof(string), true),
		new("C", typeof(double), false),
		new("D", typeof(DateTime), false)
	};

	[Fact]
	public async Task InitializeAsync_NoOptions_PassesThrough()
	{
		// Arrange
		var options = new ProjectOptions();
		var transformer = new ProjectDataTransformer(options);

		// Act
		var resultSchema = await transformer.InitializeAsync(_sourceSchema);

		// Assert
		Assert.Equal(_sourceSchema.Count, resultSchema.Count);
		for (int i = 0; i < _sourceSchema.Count; i++)
		{
			Assert.Equal(_sourceSchema[i].Name, resultSchema[i].Name);
		}

		// Check data transform
		var row = new object?[] { 1, "text", 3.14, DateTime.Now };
		var transformedRow = transformer.Transform(row);
		Assert.Equal(row, transformedRow); // Should return same reference or identical
	}

	[Fact]
	public async Task InitializeAsync_Project_SelectsColumnsAndReorders()
	{
		// Arrange
		var options = new ProjectOptions { Project = "C, A" }; // Reorder
		var transformer = new ProjectDataTransformer(options);

		// Act
		var resultSchema = await transformer.InitializeAsync(_sourceSchema);

		// Assert
		Assert.Equal(2, resultSchema.Count);
		Assert.Equal("C", resultSchema[0].Name);
		Assert.Equal("A", resultSchema[1].Name);

		// Check data transform
		var now = DateTime.Now;
		var row = new object?[] { 1, "text", 3.14, now };
		var transformedRow = transformer.Transform(row);

		Assert.Equal(2, transformedRow!.Length);
		Assert.Equal(3.14, transformedRow[0]);
		Assert.Equal(1, transformedRow[1]);
	}

	[Fact]
	public async Task InitializeAsync_Drop_RemovesColumns()
	{
		// Arrange
		var options = new ProjectOptions { Drop = "B, D" };
		var transformer = new ProjectDataTransformer(options);

		// Act
		var resultSchema = await transformer.InitializeAsync(_sourceSchema);

		// Assert
		Assert.Equal(2, resultSchema.Count);
		Assert.Equal("A", resultSchema[0].Name);
		Assert.Equal("C", resultSchema[1].Name); // Original order preserved

		// Check data transform
		var row = new object?[] { 1, "text", 3.14, DateTime.Now };
		var transformedRow = transformer.Transform(row);

		Assert.Equal(2, transformedRow!.Length);
		Assert.Equal(1, transformedRow[0]);
		Assert.Equal(3.14, transformedRow[1]);
	}

	[Fact]
	public async Task InitializeAsync_DropAndProject_DropTakesPrecedence()
	{
		// Arrange: "A" is in both Drop and Project. It should be DROPPED.
		var options = new ProjectOptions { Drop = "A", Project = "A, C" };
		var transformer = new ProjectDataTransformer(options);

		// Act
		var resultSchema = await transformer.InitializeAsync(_sourceSchema);

		// Assert
		Assert.NotNull(resultSchema);
		Assert.Single(resultSchema!);
		Assert.Equal("C", resultSchema[0].Name);

		// Check data
		var row = new object?[] { 1, "text", 3.14, DateTime.Now };
		var transformedRow = transformer.Transform(row);

		Assert.NotNull(transformedRow);
		Assert.Single(transformedRow);
		Assert.Equal(3.14, transformedRow[0]);
	}

	[Fact]
	public async Task Transform_HandlesNullSafety()
	{
		// Arrange
		var options = new ProjectOptions { Project = "A" };
		var transformer = new ProjectDataTransformer(options);
		await transformer.InitializeAsync(_sourceSchema);

		// Act
		// Pass row shorter than schema (simulate severe bug or data mismatch)
		var shortRow = new object?[] { };
		var result = transformer.Transform(shortRow);

		// Assert
		Assert.NotNull(result);
		Assert.Single(result);
		Assert.Null(result[0]); // Safe fallback
	}
}
