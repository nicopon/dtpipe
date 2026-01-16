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
        var result = await transformer.TransformAsync(rows, TestContext.Current.CancellationToken);
            
        // Assert
        result[0][0].Should().Be("invalid.dataset");
    }

    [Fact]
    public async Task Transform_ShouldKeepOriginalValue_WhenDatasetValidButMethodInvalid()
    {
        // Arrange
        // "name" is a valid dataset, but "invalidmethod" is not a method. 
        // Should NOT fallback to string, should warn and keep original.
        var options = new FakeOptions { Mappings = new[] { "NAME:name.invalidmethod" }, Seed = 123 };
        
        var columns = new List<ColumnInfo> { new("NAME", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "Original" } };

        // Act
        // Suppress error output
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
        }
        finally
        {
            Console.SetError(originalError);
        }
    }

    [Fact]
    public async Task Transform_ShouldFallbackToString_WhenNoDot()
    {
        // Arrange
        var options = new FakeOptions { Mappings = new[] { "CITY:MyFixedCity" } };
        var transformer = new FakeDataTransformer(options);
        var columns = new List<ColumnInfo> { new("CITY", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "Original" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = await transformer.TransformAsync(rows, TestContext.Current.CancellationToken);

        // Assert
        result[0][0].Should().Be("MyFixedCity");
    }

    [Fact]
    public async Task Transform_ShouldPreserveCase_WhenFallingBackToString()
    {
        // Arrange
        var options = new FakeOptions { Mappings = new[] { "EMAIL:My.Email@Domain.com" } }; // "My" is not a dataset
        var transformer = new FakeDataTransformer(options);
        var columns = new List<ColumnInfo> { new("EMAIL", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "Original" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = await transformer.TransformAsync(rows, TestContext.Current.CancellationToken);

        // Assert
        result[0][0].Should().Be("My.Email@Domain.com");
    }

    // ===== Column Reference Tests =====

    [Fact]
    public async Task Transform_ShouldSubstituteColumnReferences()
    {
        // Arrange
        var options = new FakeOptions 
        { 
            Mappings = new[] 
            { 
                "FIRSTNAME:name.firstname",
                "LASTNAME:name.lastname",
                "FULLNAME:{{FIRSTNAME}} {{LASTNAME}}"
            },
            Seed = 42
        };
        var transformer = new FakeDataTransformer(options);
        var columns = new List<ColumnInfo>
        {
            new("FIRSTNAME", typeof(string), true),
            new("LASTNAME", typeof(string), true),
            new("FULLNAME", typeof(string), true)
        };
        var rows = new List<object?[]> { new object?[] { "Old1", "Old2", "Old3" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = await transformer.TransformAsync(rows, TestContext.Current.CancellationToken);

        // Assert
        var firstName = result[0][0]?.ToString();
        var lastName = result[0][1]?.ToString();
        var fullName = result[0][2]?.ToString();

        firstName.Should().NotBeNullOrEmpty();
        lastName.Should().NotBeNullOrEmpty();
        fullName.Should().Be($"{firstName} {lastName}");
    }

    [Fact]
    public async Task Transform_ShouldHandleMultipleReferencesInTemplate()
    {
        // Arrange
        var options = new FakeOptions 
        { 
            Mappings = new[] 
            { 
                "FIRSTNAME:John",
                "LASTNAME:Doe",
                "EMAIL:{{FIRSTNAME}}.{{LASTNAME}}@company.com"
            }
        };
        var transformer = new FakeDataTransformer(options);
        var columns = new List<ColumnInfo>
        {
            new("FIRSTNAME", typeof(string), true),
            new("LASTNAME", typeof(string), true),
            new("EMAIL", typeof(string), true)
        };
        var rows = new List<object?[]> { new object?[] { "Old1", "Old2", "Old3" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = await transformer.TransformAsync(rows, TestContext.Current.CancellationToken);

        // Assert
        result[0][0].Should().Be("John");
        result[0][1].Should().Be("Doe");
        result[0][2].Should().Be("John.Doe@company.com");
    }

    [Fact]
    public async Task Transform_ShouldBeCaseInsensitive_ForColumnReferences()
    {
        // Arrange
        var options = new FakeOptions 
        { 
            Mappings = new[] 
            { 
                "firstname:John",
                "FULLNAME:{{FIRSTNAME}} Smith" // Reference with different case
            }
        };
        var transformer = new FakeDataTransformer(options);
        var columns = new List<ColumnInfo>
        {
            new("FIRSTNAME", typeof(string), true),
            new("FULLNAME", typeof(string), true)
        };
        var rows = new List<object?[]> { new object?[] { "Old1", "Old2" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = await transformer.TransformAsync(rows, TestContext.Current.CancellationToken);

        // Assert
        result[0][1].Should().Be("John Smith");
    }

    [Fact]
    public async Task Transform_ShouldSetColumnToNull_WhenNullColumnsSpecified()
    {
        // Arrange
        var options = new FakeOptions 
        { 
            NullColumns = new[] { "SENSITIVE" }
        };
        var transformer = new FakeDataTransformer(options);
        var columns = new List<ColumnInfo>
        {
            new("ID", typeof(int), false),
            new("SENSITIVE", typeof(string), true),
            new("NAME", typeof(string), true)
        };
        var rows = new List<object?[]> { new object?[] { 1, "SecretData", "John" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = await transformer.TransformAsync(rows, TestContext.Current.CancellationToken);

        // Assert
        result[0][0].Should().Be(1);           // ID unchanged
        result[0][1].Should().BeNull();        // SENSITIVE set to null
        result[0][2].Should().Be("John");      // NAME unchanged
    }
}
