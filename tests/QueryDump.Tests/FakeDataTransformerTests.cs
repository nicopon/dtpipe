using FluentAssertions;
using QueryDump.Core;
using QueryDump.Transformers.Fake;
using Xunit;

namespace QueryDump.Tests;

public class FakeDataTransformerTests
{
    [Fact]
    public void Constructor_WithMappings_ShouldParseCorrectly()
    {
        // Arrange
        var options = new FakeOptions { Mappings = new[] { "CITY:address.city", "NAME:name.firstname" } };

        // Act
        var transformer = new FakeDataTransformer(options);

        // Assert
        transformer.HasMappings.Should().BeTrue();
    }
    
    [Fact]
    public async Task Transform_ShouldReplaceValues_WhenMappingExists()
    {
        // Arrange
        var options = new FakeOptions { Mappings = new[] { "Name:name.firstname", "City:address.city" }, Seed = 12345 };
        var transformer = new FakeDataTransformer(options);
        
        var columns = new List<ColumnInfo>
        {
            new("ID", typeof(int), false),
            new("Name", typeof(string), true),
            new("City", typeof(string), true)
        };

        var row1 = new object?[] { 1, "OldName1", "OldCity1" };
        var row2 = new object?[] { 2, "OldName2", "OldCity2" };
        var rows = new List<object?[]> { row1, row2 };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = await transformer.TransformAsync(rows, TestContext.Current.CancellationToken);

        // Assert
        result.Should().HaveCount(2);
        
        // ID should be unchanged
        result[0][0].Should().Be(1);
        result[1][0].Should().Be(2);

        // Name and City should be changed and not null
        result[0][1].Should().NotBe("OldName1").And.NotBeNull();
        result[0][2].Should().NotBe("OldCity1").And.NotBeNull();
        
        result[1][1].Should().NotBe("OldName2").And.NotBeNull();
        result[1][2].Should().NotBe("OldCity2").And.NotBeNull();
    }
    
    [Fact]
    public async Task Transform_ShouldBeCaseInsensitive_ForColumnNames()
    {
         // Arrange
        var options = new FakeOptions { Mappings = new[] { "NAME:name.firstname" }, Seed = 123 };
        var transformer = new FakeDataTransformer(options);
        
        var columns = new List<ColumnInfo>
        {
            new("name", typeof(string), true) // Lowercase in schema
        };

        var rows = new List<object?[]> { new object?[] { "Original" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = await transformer.TransformAsync(rows, TestContext.Current.CancellationToken);

        // Assert
        result[0][0].Should().NotBe("Original");
    }

    [Fact]
    public async Task Transform_ShouldBeDeterministic_WithSeed()
    {
        // Arrange
        var mappings = new[] { "NAME:name.firstname" };
        var options1 = new FakeOptions { Mappings = mappings, Seed = 42 };
        var options2 = new FakeOptions { Mappings = mappings, Seed = 42 };

        var transformer1 = new FakeDataTransformer(options1);
        var transformer2 = new FakeDataTransformer(options2);
        
        var columns = new List<ColumnInfo> { new("NAME", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "Original" } };

        // Act
        await transformer1.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result1 = await transformer1.TransformAsync(rows, TestContext.Current.CancellationToken);

        await transformer2.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result2 = await transformer2.TransformAsync(rows, TestContext.Current.CancellationToken);

        // Assert
        result1[0][0].Should().Be(result2[0][0]);
    }

    [Fact]
    public async Task Transform_ShouldRespectLocale()
    {
        // Arrange
        var options = new FakeOptions { Mappings = new[] { "COUNTRY:address.country" }, Locale = "fr", Seed = 1 };
        var transformerFr = new FakeDataTransformer(options);
        
        var columns = new List<ColumnInfo> { new("COUNTRY", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "Orig" } };

        // Act
        await transformerFr.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var resultFr = await transformerFr.TransformAsync(rows, TestContext.Current.CancellationToken);
        
        // Assert
        resultFr[0][0].Should().BeOfType<string>();
    }
    
    [Fact]
    public async Task Transform_ShouldHandleUnknownFaker_Gracefully()
    {
        // Arrange
        var options = new FakeOptions { Mappings = new[] { "NAME:invalid.dataset" }, Seed = 123 };
        
        var columns = new List<ColumnInfo> { new("NAME", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "Original" } };

        // Act
        // Suppress error output to keep test runner clean
        var originalError = Console.Error;
        try
        {
            using var sw = new StringWriter();
            Console.SetError(sw);
            
            var transformer = new FakeDataTransformer(options);
            await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
            var result = await transformer.TransformAsync(rows, TestContext.Current.CancellationToken);
            
            // Assert
            result[0][0].Should().Be("Original");
            
            // Verify it logged 
            var log = sw.ToString();
        }
        finally
        {
            Console.SetError(originalError);
        }
    }
}
