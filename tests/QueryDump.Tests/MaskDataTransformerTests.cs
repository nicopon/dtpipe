using FluentAssertions;
using QueryDump.Core;
using QueryDump.Transformers.Mask;
using Xunit;

namespace QueryDump.Tests;

public class MaskDataTransformerTests
{
    [Fact]
    public async Task Transform_ShouldMaskData_UsingPattern()
    {
        // Arrange
        var options = new MaskOptions { Mappings = new[] { "EMAIL:###****" } };
        var transformer = new MaskDataTransformer(options);
        var columns = new List<ColumnInfo> { new("EMAIL", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "test@example.com" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();

        // Assert
        // "tes" kept (#), "****" replaced, "ample.com" unmatched kept
        result[0][0].Should().Be("tes****ample.com");
    }

    [Fact]
    public async Task Transform_ShouldMaskData_FullReplacement()
    {
        // Arrange
        var options = new MaskOptions { Mappings = new[] { "PIN:****" } };
        var transformer = new MaskDataTransformer(options);
        var columns = new List<ColumnInfo> { new("PIN", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "1234" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();

        // Assert
        result[0][0].Should().Be("****");
    }

    [Fact]
    public async Task Transform_ShouldMaskData_WithComplexPattern()
    {
        // Arrange
        // Pattern length matches input length roughly. 
        // Logic is 1:1 replacement. 
        // Input: 0612345678 (10 chars)
        // Pattern: ##******## (10 chars) -> Keep first 2, mask middle 6, keep last 2
        var options = new MaskOptions { Mappings = new[] { "PHONE:##******##" } };
        var transformer = new MaskDataTransformer(options);
        var columns = new List<ColumnInfo> { new("PHONE", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "0612345678" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();

        // Assert
        result[0][0].Should().Be("06******78");
    }

    [Fact]
    public async Task Transform_ShouldHandleNullValues_ByDefault()
    {
        // Arrange - Default behavior (replace with pattern chars if pattern length allows, or keep null?)
        // Mask logic: if value is null, result is null unless specific handling
        // Let's check implementation: Transform checks "if (value is string str)"
        // So non-string or null values are preserved by default logic.
        
        var options = new MaskOptions { Mappings = new[] { "COL:***" } };
        var transformer = new MaskDataTransformer(options);
        var columns = new List<ColumnInfo> { new("COL", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { null } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();

        // Assert
        result[0][0].Should().BeNull("Mask works on strings, so nulls should pass through naturally unless SkipNull changes pipeline behavior");
    }

    [Fact]
    public async Task Transform_ShouldSkipFormatting_WhenSkipNullEnabled_AndValueNull()
    {
        // Arrange
        var options = new MaskOptions { Mappings = new[] { "COL:***" }, SkipNull = true };
        var transformer = new MaskDataTransformer(options);
        var columns = new List<ColumnInfo> { new("COL", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { null } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();

        // Assert
        result[0][0].Should().BeNull();
    }
}
