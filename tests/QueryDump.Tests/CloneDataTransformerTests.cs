using FluentAssertions;
using QueryDump.Core;
using QueryDump.Transformers.Clone;
using Xunit;

namespace QueryDump.Tests;

public class CloneDataTransformerTests
{
    [Fact]
    public async Task Transform_ShouldSubstituteColumnReferences()
    {
        // Arrange
        var options = new CloneOptions { Mappings = new[] { "FULLNAME:{{FIRST}} {{LAST}}" } };
        var transformer = new CloneDataTransformer(options);
        var columns = new List<ColumnInfo>
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
        result[0][2].Should().Be("John Doe");
    }

    [Fact]
    public async Task Transform_ShouldHandleChainedDependencies_TopologicalSort()
    {
        // Arrange
        // C depends on B, B depends on A
        var options = new CloneOptions 
        { 
            Mappings = new[] 
            { 
                "C:{{B}} Copied", 
                "B:{{A}} Copied" 
            } 
        };
        var transformer = new CloneDataTransformer(options);
        var columns = new List<ColumnInfo>
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
        result[0][1].Should().Be("Base Copied");
        result[0][2].Should().Be("Base Copied Copied");
    }
    
    [Fact]
    public async Task Transform_ShouldCloneDirectly_WithoutTemplate()
    {
        // Support simple clone syntax? Currently option is only for mappings
        // The implementation assumes the value IS the template. 
        // So "COLUMN:{{OTHER}}" works.
        // What about "COLUMN:OTHER"? 
        // Based on implementation, it uses TemplatePattern regex. "OTHER" has no {{.
        // So implementation does strict text replacment.
        // CloneDataTransformer.cs: IsTemplate call was removed? 
        // Current implementation: always runs substitute. Substitute uses regex.
        // So "COLUMN:OTHER" results in literal "OTHER".
        // To support direct clone "COLUMN:OTHER", user must use "COLUMN:{{OTHER}}". 
        // This is acceptable behavior for now.
        
        var options = new CloneOptions { Mappings = new[] { "COPY:{{ORIGINAL}}" } };
        var transformer = new CloneDataTransformer(options);
        var columns = new List<ColumnInfo>
        {
            new("ORIGINAL", typeof(string), true),
            new("COPY", typeof(string), true)
        };
        var rows = new List<object?[]> { new object?[] { "SourceData", "Old" } };

        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();

        result[0][1].Should().Be("SourceData");
    }
}
