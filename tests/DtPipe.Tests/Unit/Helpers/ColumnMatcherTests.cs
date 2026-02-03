using DtPipe.Core.Helpers;
using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Helpers;

public class ColumnMatcherTests
{
    // --- ResolvePhysicalName Tests ---
    
    [Fact]
    public void ResolvePhysicalName_NoDialect_ReturnsOriginalName()
    {
        // Arrange
        var input = "MyColumn";
        
        // Act
        var result = ColumnMatcher.ResolvePhysicalName(input, false, null);
        
        // Assert
        Assert.Equal(input, result); // No normalization without dialect
    }
    
    [Fact]
    public void ResolvePhysicalName_CaseSensitive_PreservesCase()
    {
        // Arrange - case-sensitive column should never be normalized
        var input = "MyColumn";
        
        // Act
        var result = ColumnMatcher.ResolvePhysicalName(input, true, null);
        
        // Assert
        Assert.Equal("MyColumn", result);
    }
    
    // --- FindMatchingColumnCaseInsensitive Tests ---
    
    [Fact]
    public void FindMatchingColumnCaseInsensitive_Match_ReturnsColumn()
    {
        // Arrange
        var columns = new[] { "id", "Name", "EMAIL" };
        
        // Act
        var result = ColumnMatcher.FindMatchingColumnCaseInsensitive(
            "name",
            columns,
            c => c);
        
        // Assert
        Assert.Equal("Name", result);
    }
    
    [Fact]
    public void FindMatchingColumnCaseInsensitive_UppercaseInput_ReturnsColumn()
    {
        // Arrange
        var columns = new[] { "id", "name", "email" };
        
        // Act
        var result = ColumnMatcher.FindMatchingColumnCaseInsensitive(
            "NAME",
            columns,
            c => c);
        
        // Assert
        Assert.Equal("name", result);
    }
    
    [Fact]
    public void FindMatchingColumnCaseInsensitive_NoMatch_ReturnsNull()
    {
        // Arrange
        var columns = new[] { "id", "name", "email" };
        
        // Act
        var result = ColumnMatcher.FindMatchingColumnCaseInsensitive(
            "nonexistent",
            columns,
            c => c);
        
        // Assert
        Assert.Null(result);
    }
    
    [Fact]
    public void FindMatchingColumnCaseInsensitive_WithColumnInfo_ReturnsColumn()
    {
        // Arrange
        var columns = new List<ColumnInfo>
        {
            new ColumnInfo("id", typeof(int), false),
            new ColumnInfo("Name", typeof(string), false),
            new ColumnInfo("Email", typeof(string), false)
        };
        
        // Act
        var result = ColumnMatcher.FindMatchingColumnCaseInsensitive(
            "name",
            columns,
            c => c.Name);
        
        // Assert
        Assert.NotNull(result);
        Assert.Equal("Name", result.Name);
    }
    
    [Fact]
    public void FindMatchingColumnCaseInsensitive_MultipleMatches_ReturnsFirst()
    {
        // Arrange - should never happen in practice but test behavior
        var columns = new[] { "id", "name", "NAME" };
        
        // Act
        var result = ColumnMatcher.FindMatchingColumnCaseInsensitive(
            "name",
            columns,
            c => c);
        
        // Assert - returns first match (case-insensitive)
        Assert.Equal("name", result);
    }
    
    // --- EdgeCases ---
    
    [Fact]
    public void FindMatchingColumnCaseInsensitive_EmptyList_ReturnsNull()
    {
        // Arrange
        var columns = new List<ColumnInfo>();
        
        // Act
        var result = ColumnMatcher.FindMatchingColumnCaseInsensitive(
            "anyColumn",
            columns,
            c => c.Name);
        
        // Assert
        Assert.Null(result);
    }
    
    [Fact]
    public void FindMatchingColumnCaseInsensitive_EmptyString_NoMatch()
    {
        // Arrange
        var columns = new[] { "id", "name", "" };
        
        // Act
        var result = ColumnMatcher.FindMatchingColumnCaseInsensitive(
            "",
            columns,
            c => c);
        
        // Assert
        Assert.Equal("", result); // Matches empty string
    }
    
    [Fact]
    public void FindMatchingColumnCaseInsensitive_SpecialCharacters_Match()
    {
        // Arrange
        var columns = new List<ColumnInfo>
        {
            new ColumnInfo("user-id", typeof(int), false, true), // Case-sensitive
            new ColumnInfo("normal_col", typeof(string), false)
        };
        
        // Act
        var result = ColumnMatcher.FindMatchingColumnCaseInsensitive(
            "USER-ID",
            columns,
            c => c.Name);
        
        // Assert - case-insensitive should match despite case difference
        Assert.NotNull(result);
        Assert.Equal("user-id", result.Name);
    }
}
