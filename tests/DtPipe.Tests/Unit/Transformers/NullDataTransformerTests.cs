using FluentAssertions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Cli.Abstractions;
using DtPipe.Transformers.Null;
using Xunit;

namespace DtPipe.Tests;

public class NullDataTransformerTests
{
    [Fact]
    public async Task Transform_ShouldSetColumnToNull_WhenNullColumnsSpecified()
    {
        // Arrange
        var options = new NullOptions { Columns = new[] { "SENSITIVE" } };
        var transformer = new NullDataTransformer(options);
        var columns = new List<ColumnInfo>
        {
            new("ID", typeof(int), false),
            new("SENSITIVE", typeof(string), true),
            new("NAME", typeof(string), true)
        };
        var rows = new List<object?[]> { new object?[] { 1, "SecretData", "John" } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();

        // Assert
        result[0][0].Should().Be(1);           // ID unchanged
        result[0][1].Should().BeNull();        // SENSITIVE set to null
        result[0][2].Should().Be("John");      // NAME unchanged
    }

    [Fact]
    public async Task Transform_ShouldDoNothing_WhenNoNullColumnsMatch()
    {
        // Arrange
        var options = new NullOptions { Columns = new[] { "NONEXISTENT" } };
        var transformer = new NullDataTransformer(options);
        var columns = new List<ColumnInfo> { new("ID", typeof(int), false) };
        var rows = new List<object?[]> { new object?[] { 1 } };

        // Act
        await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        var result = rows.Select(r => transformer.Transform(r)).ToList();

        // Assert
        result[0][0].Should().Be(1);
    }
}
