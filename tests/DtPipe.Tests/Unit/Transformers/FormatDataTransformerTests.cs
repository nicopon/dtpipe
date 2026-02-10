using DtPipe.Cli.Abstractions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Transformers.Format;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests;

public class FormatDataTransformerTests
{
	[Fact]
	public async Task Transform_ShouldSubstituteColumnReferences()
	{
		// Arrange
		var options = new FormatOptions { Format = new[] { "FULLNAME:{FIRST} {LAST}" } };
		var transformer = new FormatDataTransformer(options);
		var columns = new List<PipeColumnInfo>
		{
			new("FIRST", typeof(string), true),
			new("LAST", typeof(string), true),
			new("FULLNAME", typeof(string), true)
		};
		var rows = new List<object?[]> { new object?[] { "John", "Doe", "OldName" } };

		// Act
		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var result = rows.Select(r => transformer.Transform(r)).ToList();

		// Assert
		result[0]![2].Should().Be("John Doe");
	}

	[Fact]
	public async Task Transform_ShouldHandleChainedDependencies_TopologicalSort()
	{
		// Arrange
		// C depends on B, B depends on A
		var options = new FormatOptions
		{
			Format = new[]
			{
				"C:{B} Copied",
				"B:{A} Copied"
			}
		};
		var transformer = new FormatDataTransformer(options);
		var columns = new List<PipeColumnInfo>
		{
			new("A", typeof(string), true),
			new("B", typeof(string), true),
			new("C", typeof(string), true)
		};
		// Verify dependency order: B must be processed before C
		// Initial: A="Base", B="Old", C="Old"
		// Step 1 (B): B = "Base Copied"
		// Step 2 (C): C = "Base Copied Copied"
		var rows = new List<object?[]> { new object?[] { "Base", "Old", "Old" } };

		// Act
		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var result = rows.Select(r => transformer.Transform(r)).ToList();

		// Assert
		result[0]![1].Should().Be("Base Copied");
		result[0]![2].Should().Be("Base Copied Copied");
	}

	[Fact]
	public async Task Transform_ShouldFormatWithDirectCopy()
	{
		var options = new FormatOptions { Format = new[] { "COPY:{ORIGINAL}" } };
		var transformer = new FormatDataTransformer(options);
		var columns = new List<PipeColumnInfo>
		{
			new("ORIGINAL", typeof(string), true),
			new("COPY", typeof(string), true)
		};
		var rows = new List<object?[]> { new object?[] { "SourceData", "Old" } };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var result = rows.Select(r => transformer.Transform(r)).ToList();

		result[0]![1].Should().Be("SourceData");
	}

	[Fact]
	public async Task Transform_ShouldApplyFormatSpecifier_ForNumbers()
	{
		// Arrange: Use {COLUMN:format} syntax
		var options = new FormatOptions { Format = new[] { "PRICE_FMT:{PRICE:0.00}" } };
		var transformer = new FormatDataTransformer(options);
		var columns = new List<PipeColumnInfo>
		{
			new("PRICE", typeof(decimal), true),
			new("PRICE_FMT", typeof(string), true)
		};
		var rows = new List<object?[]> { new object?[] { 123.456m, "Old" } };

		// Act
		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var result = rows.Select(r => transformer.Transform(r)).ToList();

		// Assert: Should be formatted to 2 decimal places
		result[0]![1].Should().Be("123.46");
	}

	[Fact]
	public async Task Transform_ShouldApplyFormatSpecifier_ForDates()
	{
		// Arrange: Date formatting
		var options = new FormatOptions { Format = new[] { "DATE_FR:{DATE:dd/MM/yyyy}" } };
		var transformer = new FormatDataTransformer(options);
		var columns = new List<PipeColumnInfo>
		{
			new("DATE", typeof(DateTime), true),
			new("DATE_FR", typeof(string), true)
		};
		var rows = new List<object?[]> { new object?[] { new DateTime(2024, 1, 15), "Old" } };

		// Act
		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var result = rows.Select(r => transformer.Transform(r)).ToList();

		// Assert
		result[0]![1].Should().Be("15/01/2024");
	}

	[Fact]
	public async Task Transform_ShouldApplyFormatSpecifier_ForIntegerPadding()
	{
		// Arrange: Padding with zeros
		var options = new FormatOptions { Format = new[] { "CODE_PADDED:{CODE:D6}" } };
		var transformer = new FormatDataTransformer(options);
		var columns = new List<PipeColumnInfo>
		{
			new("CODE", typeof(int), true),
			new("CODE_PADDED", typeof(string), true)
		};
		var rows = new List<object?[]> { new object?[] { 42, "Old" } };

		// Act
		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var result = rows.Select(r => transformer.Transform(r)).ToList();

		// Assert: Should be "000042"
		result[0]![1].Should().Be("000042");
	}

	[Fact]
	public async Task Transform_ShouldCombineBothSyntaxes()
	{
		// Arrange: Mix {COLUMN:format} and {COLUMN}
		var options = new FormatOptions { Format = new[] { "LABEL:{PRICE:0.00}€ - {NAME}" } };
		var transformer = new FormatDataTransformer(options);
		var columns = new List<PipeColumnInfo>
		{
			new("PRICE", typeof(decimal), true),
			new("NAME", typeof(string), true),
			new("LABEL", typeof(string), true)
		};
		var rows = new List<object?[]> { new object?[] { 99.5m, "Product", "Old" } };

		// Act
		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var result = rows.Select(r => transformer.Transform(r)).ToList();

		// Assert
		result[0]![2].Should().Be("99.50€ - Product");
	}
	[Fact]
	public async Task Transform_ShouldSkipFormat_OnlyWhenAllSourceColumnsAreNull()
	{
		// Arrange
		// Template uses A and B. SkipNull set to true.
		var options = new FormatOptions
		{
			Format = new[] { "RESULT:{A}-{B}" },
			SkipNull = true
		};
		var transformer = new FormatDataTransformer(options);
		var columns = new List<PipeColumnInfo>
		{
			new("A", typeof(string), true),
			new("B", typeof(string), true),
			new("RESULT", typeof(string), true)
		};

		var rows = new List<object?[]>
		{
			new object?[] { null, null, "Old" },  // Case 1: Both null -> Skip (Result=Null)
            new object?[] { "ValA", null, "Old" },// Case 2: One present -> Format (Result="ValA-")
            new object?[] { null, "ValB", "Old" } // Case 3: Other present -> Format (Result="-ValB")
        };

		// Act
		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var result = rows.Select(r => transformer.Transform(r)).ToList();

		// Assert
		// Case 1: All sources are null -> Result set to NULL
		result[0]![2].Should().BeNull("All sources are null, so format is skipped and target is nulled");

		// Case 2: Mixed -> Formatted
		result[1]![2].Should().Be("ValA-");

		// Case 3: Mixed -> Formatted
		result[2]![2].Should().Be("-ValB");
	}
}
