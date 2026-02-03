using Xunit;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.DryRun;
using Moq;

namespace DtPipe.Tests.Integration;

public class DryRunKeyValidationTests
{
    private class MockKeyValidator : ISchemaInspector, IKeyValidator, IHasSqlDialect
    {
        public bool IsRequired { get; set; } = true;
        public List<string> RequestedKeys { get; set; } = new();
        public ISqlDialect Dialect { get; set; } = null!;
        public List<ColumnInfo> TargetColumns { get; set; } = new();

        public Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
        {
            var cols = TargetColumns.Select(c => new TargetColumnInfo(
                c.Name, "VARCHAR", typeof(string), true, false, false)).ToList();
            
            return Task.FromResult<TargetSchemaInfo?>(
                new TargetSchemaInfo(cols, true, 0, 0, null)
            );
        }

        public string? GetWriteStrategy() => "MockStrategy";
        public IReadOnlyList<string>? GetRequestedPrimaryKeys() => RequestedKeys;
        public bool RequiresPrimaryKey() => IsRequired;
    }

    private async IAsyncEnumerable<ReadOnlyMemory<object?[]>> GetSampleDataBatches()
    {
        // Batch 1: Just one batch with 2 rows
        var rows = new object?[][] 
        {
            new object?[] { 1, "Alice" },
            new object?[] { 2, "Bob" }
        };
        
        yield return new ReadOnlyMemory<object?[]>(rows);
        await Task.CompletedTask;
    }

    private void SetupReader(Mock<IStreamReader> readerMock)
    {
        // Setup Columns
        var cols = new List<ColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true)
        };
        readerMock.Setup(r => r.Columns).Returns(cols);
        
        // Setup ReadBatchesAsync
        readerMock.Setup(r => r.ReadBatchesAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                  .Returns(GetSampleDataBatches());
                  
        // Setup OpenAsync
        readerMock.Setup(r => r.OpenAsync(It.IsAny<CancellationToken>()))
                  .Returns(Task.CompletedTask);
    }

    [Fact]
    public async Task Validate_ValidKey_ReturnsSuccess()
    {
        // Arrange
        var analyzer = new DryRunAnalyzer();
        var reader = new Mock<IStreamReader>();
        SetupReader(reader);

        var writer = new MockKeyValidator
        {
            IsRequired = true,
            RequestedKeys = new List<string> { "id" },
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect()
        };
        writer.TargetColumns = new List<ColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true)
        };

        // Act
        var result = await analyzer.AnalyzeAsync(
            reader.Object, 
            new List<IDataTransformer>(), 
            10, 
            writer);

        // Assert
        Assert.NotNull(result.KeyValidation);
        Assert.True(result.KeyValidation.IsValid);
        Assert.Equal("id", result.KeyValidation.ResolvedKeys![0]);
        Assert.Empty(result.KeyValidation.Errors!);
    }

    [Fact]
    public async Task Validate_InvalidKey_ReturnsError()
    {
        // Arrange
        var analyzer = new DryRunAnalyzer();
        var reader = new Mock<IStreamReader>();
        SetupReader(reader);

        var writer = new MockKeyValidator
        {
            IsRequired = true,
            RequestedKeys = new List<string> { "invalid_col" },
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect()
        };
        writer.TargetColumns = new List<ColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true)
        };

        // Act
        var result = await analyzer.AnalyzeAsync(
            reader.Object, 
            new List<IDataTransformer>(), 
            10, 
            writer);

        // Assert
        Assert.NotNull(result.KeyValidation);
        Assert.False(result.KeyValidation.IsValid);
        Assert.Contains("not found in final schema", result.KeyValidation.Errors![0]);
    }

    [Fact]
    public async Task Validate_CaseMismatch_ResolvesCorrectly()
    {
        // Arrange
        var analyzer = new DryRunAnalyzer();
        var reader = new Mock<IStreamReader>();
        SetupReader(reader);

        var writer = new MockKeyValidator
        {
            IsRequired = true,
            RequestedKeys = new List<string> { "ID" }, // Uppercase request
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect()
        };
        writer.TargetColumns = new List<ColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true)
        };

        // Act
        var result = await analyzer.AnalyzeAsync(
            reader.Object, 
            new List<IDataTransformer>(), 
            10, 
            writer);

        // Assert
        Assert.NotNull(result.KeyValidation);
        Assert.True(result.KeyValidation.IsValid);
        Assert.Equal("id", result.KeyValidation.ResolvedKeys![0]); // Resolved to lowercase "id"
    }

    [Fact]
    public async Task Validate_NotRequired_ReturnsValid()
    {
        // Arrange
        var analyzer = new DryRunAnalyzer();
        var reader = new Mock<IStreamReader>();
        SetupReader(reader);

        var writer = new MockKeyValidator
        {
            IsRequired = false, // Not required (e.g. Recreate strategy)
            RequestedKeys = new List<string>(),
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect()
        };
        writer.TargetColumns = new List<ColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true)
        };

        // Act
        var result = await analyzer.AnalyzeAsync(
            reader.Object, 
            new List<IDataTransformer>(), 
            10, 
            writer);

        // Assert
        Assert.NotNull(result.KeyValidation);
        Assert.False(result.KeyValidation.IsRequired);
        Assert.True(result.KeyValidation.IsValid);
    }
    
    [Fact]
    public async Task Validate_MissingRequiredKey_ReturnsError()
    {
        // Arrange
        var analyzer = new DryRunAnalyzer();
        var reader = new Mock<IStreamReader>();
        SetupReader(reader);

        var writer = new MockKeyValidator
        {
            IsRequired = true, 
            RequestedKeys = new List<string>(), // Empty!
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect()
        };
        writer.TargetColumns = new List<ColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true)
        };

        // Act
        var result = await analyzer.AnalyzeAsync(
            reader.Object, 
            new List<IDataTransformer>(), 
            10, 
            writer);

        // Assert
        Assert.NotNull(result.KeyValidation);
        Assert.True(result.KeyValidation.IsRequired);
        Assert.False(result.KeyValidation.IsValid);
        Assert.Contains("requires a primary key", result.KeyValidation.Errors![0]);
    }
}
