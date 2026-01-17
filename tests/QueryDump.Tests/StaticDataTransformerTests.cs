using FluentAssertions;
using QueryDump.Core;
using QueryDump.Transformers.Static;
using Xunit;

namespace QueryDump.Tests;

public class StaticDataTransformerTests
{
    [Fact]
    public async Task Transform_ShouldOverwriteColumn_WhenMappingExists()
    {
        // Arrange
        var options = new OverwriteOptions { Mappings = new[] { "CITY:Paris" } };
        var transformer = new StaticDataTransformer(options);
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
        var options = new OverwriteOptions { Mappings = new[] { "UNKNOWN:Value" } };
        var transformer = new StaticDataTransformer(options);
        var columns = new List<ColumnInfo> { new("CITY", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "London" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();

        // Assert
        result[0][0].Should().Be("London");
    }
}
