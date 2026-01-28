using FluentAssertions;
using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Cli.Abstractions;
using QueryDump.Transformers.Overwrite;
using Xunit;

namespace QueryDump.Tests;

public class OverwriteDataTransformerTests
{
    [Fact]
    public async Task Transform_ShouldOverwriteColumn_WhenMappingExists()
    {
        // Arrange
        var options = new OverwriteOptions { Overwrite = new[] { "CITY:Paris" } };
        var transformer = new OverwriteDataTransformer(options);
        var columns = new List<ColumnInfo> { new("CITY", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "London" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();

        // Assert
        result[0][0].Should().Be("Paris");
    }

    [Fact]
    public async Task Transform_ShouldIgnore_WhenColumnDoesNotExist()
    {
        // Arrange
        var options = new OverwriteOptions { Overwrite = new[] { "UNKNOWN:Value" } };
        var transformer = new OverwriteDataTransformer(options);
        var columns = new List<ColumnInfo> { new("CITY", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "London" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();

        // Assert
        result[0][0].Should().Be("London");
    }
    [Fact]
    public async Task Transform_ShouldSkipOverwrite_WhenSkipNullEnabled_AndValueIsNull()
    {
        // Arrange
        var options = new OverwriteOptions { Overwrite = new[] { "CITY:Paris" }, SkipNull = true };
        var transformer = new OverwriteDataTransformer(options);
        var columns = new List<ColumnInfo> { new("CITY", typeof(string), true) };
        var rows = new List<object?[]> 
        { 
            new object?[] { null }, 
            new object?[] { "London" } 
        };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();

        // Assert
        result[0][0].Should().BeNull("Should not overwrite null because SkipNull is true");
        result[1][0].Should().Be("Paris", "Should still overwrite non-null values");
    }
}
