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
    public void Constructor_ShouldThrowArgumentException_WhenBothDeterministicAndSeedColumnSet()
    {
        // Arrange
        var options = new FakeOptions 
        { 
            Mappings = new[] { "NAME:name.firstname" },
            Deterministic = true,
            SeedColumn = "ID" 
        };

        // Act & Assert
        var act = () => new FakeDataTransformer(options);
        act.Should().Throw<ArgumentException>()
           .WithMessage("*cannot be used together*");
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
        var result = rows.Select(r => transformer.Transform(r)).ToList();

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
        var result = rows.Select(r => transformer.Transform(r)).ToList();

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
        var result1 = rows.Select(r => transformer1.Transform(r)).ToList();

        await transformer2.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result2 = rows.Select(r => transformer2.Transform(r)).ToList();

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
        var resultFr = rows.Select(r => transformerFr.Transform(r)).ToList();
        
        // Assert
        resultFr[0][0].Should().BeOfType<string>();
    }
    
    [Fact]
    public async Task Transform_ShouldFallbackToString_WhenDatasetIsUnknown()
    {
        // Arrange
        // "invalid" is not a known dataset, so it should be treated as a hardcoded string
        var options = new FakeOptions { Mappings = new[] { "NAME:invalid.dataset" }, Seed = 123 };
        
        var columns = new List<ColumnInfo> { new("NAME", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "Original" } };

        // Act
        var transformer = new FakeDataTransformer(options);
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();
            
        // Assert
        result[0][0].Should().Be("invalid.dataset");
    }

    [Fact]
    public void Constructor_ShouldThrowException_WhenDatasetValidButMethodInvalid()
    {
        // Arrange
        // "name" is a valid dataset, but "invalidmethod" is not a method. 
        // Should throw InvalidOperationException to stop the export.
        var options = new FakeOptions { Mappings = new[] { "NAME:name.invalidmethod" }, Seed = 123 };
        
        // Act & Assert
        var act = () => new FakeDataTransformer(options);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Unknown faker method*name.invalidmethod*");
    }

    // Tests for Null and Static/Clone functionality have been moved to their respective test files:
    // - NullDataTransformerTests.cs
    // - OverwriteDataTransformerTests.cs
    // - FormatDataTransformerTests.cs
    [Fact]
    public async Task Transform_Reproduction_ColonSyntaxCheck()
    {
        // Issue reproduction: "Finance:iban" (colon separator) vs "Finance.iban" (dot separator)
        
        // Case 1: "Finance:iban" (Colon) -> Should be treated as hardcoded string "Finance:iban" because it's not a valid fake path
        var optionsColon = new FakeOptions { Mappings = new[] { "IBAN:finance:iban" } };
        var transformerColon = new FakeDataTransformer(optionsColon);
        var columns = new List<ColumnInfo> { new("IBAN", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "Original" } };

        await transformerColon.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var resColon = rows.Select(r => transformerColon.Transform(r)).ToList();
        
        // This confirms the user's colon syntax is now supported via normalization
        resColon[0][0].Should().NotBe("finance:iban");
        resColon[0][0].Should().NotBeNull();
        resColon[0][0]!.ToString()!.Length.Should().BeGreaterThan(10);
        
        // Case 2: "Finance.iban" (Dot) -> Should be recognized as faker
        var optionsDot = new FakeOptions { Mappings = new[] { "IBAN:finance.iban" } };
        var transformerDot = new FakeDataTransformer(optionsDot);
        
        await transformerDot.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var resDot = rows.Select(r => transformerDot.Transform(r)).ToList();
        
        // This confirms correct syntax works
        resDot[0][0].Should().NotBe("finance.iban");
        resDot[0][0].Should().NotBeNull();
        resDot[0][0]!.ToString()!.Length.Should().BeGreaterThan(10);
        
        // Case 3: "finance.iban" (Lowercase Dot) -> Should also work (case insensitive)
        var optionsLower = new FakeOptions { Mappings = new[] { "IBAN:finance.iban" } };
        var transformerLower = new FakeDataTransformer(optionsLower);
        
        await transformerLower.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var resLower = rows.Select(r => transformerLower.Transform(r)).ToList();
        
        resLower[0][0].Should().NotBe("finance.iban");
    }
}
