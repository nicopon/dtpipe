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
        public List<string>? TargetPKs { get; set; } = null; // Phase 2: Target PKs

        public Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
        {
            var cols = TargetColumns.Select(c => new TargetColumnInfo(
                c.Name, "VARCHAR", typeof(string), true, false, false)).ToList();
            
            return Task.FromResult<TargetSchemaInfo?>(
                new TargetSchemaInfo(cols, true, 0, 0, TargetPKs)
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
            new("name", typeof(string), true),
            new("tenant_id", typeof(int), false) // Added tenant_id for composite tests
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
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect(),
            TargetPKs = new List<string> { "id" } // Target agrees
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
        Assert.Null(result.KeyValidation.Errors); // Errors should be null if valid
    }

    [Fact]
    public async Task Validate_InvalidKey_ReturnsError()
    {
        // ... (previous test implementation remains same but careful with TargetPKs default which is null so no check)
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

    // ... (Other Phase 1 tests) ... Keeping simplified here for replacement context, 
    // actually I should preserve them. I will include them in the replace block content to avoid deleting them.
    
    [Fact]
    public async Task Validate_CaseMismatch_ResolvesCorrectly()
    {
        var analyzer = new DryRunAnalyzer();
        var reader = new Mock<IStreamReader>();
        SetupReader(reader);

        var writer = new MockKeyValidator
        {
            IsRequired = true,
            RequestedKeys = new List<string> { "ID" }, 
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect(),
            TargetPKs = new List<string> { "id" }
        };
        writer.TargetColumns = new List<ColumnInfo> { new("id", typeof(int), false) };

        var result = await analyzer.AnalyzeAsync(reader.Object, new List<IDataTransformer>(), 10, writer);

        Assert.NotNull(result.KeyValidation);
        Assert.True(result.KeyValidation.IsValid);
        Assert.Equal("id", result.KeyValidation.ResolvedKeys![0]); 
    }

    [Fact]
    public async Task Validate_NotRequired_ReturnsValid()
    {
        var analyzer = new DryRunAnalyzer();
        var reader = new Mock<IStreamReader>();
        SetupReader(reader);

        var writer = new MockKeyValidator
        {
            IsRequired = false, 
            RequestedKeys = new List<string>(),
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect()
        };
        writer.TargetColumns = new List<ColumnInfo> { new("id", typeof(int), false) };

        var result = await analyzer.AnalyzeAsync(reader.Object, new List<IDataTransformer>(), 10, writer);

        Assert.NotNull(result.KeyValidation);
        Assert.False(result.KeyValidation.IsRequired);
        Assert.True(result.KeyValidation.IsValid);
    }
    
    [Fact]
    public async Task Validate_MissingRequiredKey_ReturnsError()
    {
        var analyzer = new DryRunAnalyzer();
        var reader = new Mock<IStreamReader>();
        SetupReader(reader);

        var writer = new MockKeyValidator
        {
            IsRequired = true, 
            RequestedKeys = new List<string>(), 
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect()
        };
        writer.TargetColumns = new List<ColumnInfo> { new("id", typeof(int), false) };

        var result = await analyzer.AnalyzeAsync(reader.Object, new List<IDataTransformer>(), 10, writer);

        Assert.NotNull(result.KeyValidation);
        Assert.True(result.KeyValidation.IsRequired);
        Assert.False(result.KeyValidation.IsValid);
        Assert.Contains("requires a primary key", result.KeyValidation.Errors![0]);
    }

    // --- Phase 2 Tests ---

    [Fact]
    public async Task Validate_CompositeKeySuccess_MatchesTarget()
    {
        var analyzer = new DryRunAnalyzer();
        var reader = new Mock<IStreamReader>();
        SetupReader(reader);

        var writer = new MockKeyValidator
        {
            IsRequired = true,
            RequestedKeys = new List<string> { "id", "tenant_id" },
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect(),
            TargetPKs = new List<string> { "id", "tenant_id" } // Exact match
        };
        writer.TargetColumns = new List<ColumnInfo> 
        { 
            new("id", typeof(int), false),
            new("tenant_id", typeof(int), false)
        };

        var result = await analyzer.AnalyzeAsync(reader.Object, new List<IDataTransformer>(), 10, writer);

        Assert.NotNull(result.KeyValidation);
        Assert.True(result.KeyValidation.IsValid);
        Assert.Equal(2, result.KeyValidation.ResolvedKeys!.Count);
    }

    [Fact]
    public async Task Validate_TargetPKMismatch_MissingKey_ReturnsError()
    {
        // User provides (id), Target requires (id, tenant_id)
        var analyzer = new DryRunAnalyzer();
        var reader = new Mock<IStreamReader>();
        SetupReader(reader);

        var writer = new MockKeyValidator
        {
            IsRequired = true,
            RequestedKeys = new List<string> { "id" },
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect(),
            TargetPKs = new List<string> { "id", "tenant_id" } // Mismatch
        };
        writer.TargetColumns = new List<ColumnInfo> 
        { 
            new("id", typeof(int), false),
            new("tenant_id", typeof(int), false)
        };

        var result = await analyzer.AnalyzeAsync(reader.Object, new List<IDataTransformer>(), 10, writer);

        Assert.NotNull(result.KeyValidation);
        Assert.False(result.KeyValidation.IsValid);
        Assert.Contains("Missing: tenant_id", result.KeyValidation.Errors![0]);
    }

    [Fact]
    public async Task Validate_TargetPKMismatch_ExtraUserKey_ReturnsWarning()
    {
        // User provides (id, tenant_id), Target requires (id)
        var analyzer = new DryRunAnalyzer();
        var reader = new Mock<IStreamReader>();
        SetupReader(reader);

        var writer = new MockKeyValidator
        {
            IsRequired = true,
            RequestedKeys = new List<string> { "id", "tenant_id" },
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect(),
            TargetPKs = new List<string> { "id" } // Target is simpler
        };
        writer.TargetColumns = new List<ColumnInfo> 
        { 
            new("id", typeof(int), false),
            new("tenant_id", typeof(int), false)
        };

        var result = await analyzer.AnalyzeAsync(reader.Object, new List<IDataTransformer>(), 10, writer);

        Assert.NotNull(result.KeyValidation);
        Assert.True(result.KeyValidation.IsValid); // Still valid!
        Assert.NotNull(result.KeyValidation.Warnings);
        Assert.NotEmpty(result.KeyValidation.Warnings);
        Assert.Contains("User key includes columns not present in target", result.KeyValidation.Warnings![0]);
    }

    [Fact]
    public async Task Validate_TargetHasNoPK_ReturnsWarning()
    {
        // User provides (id), Target exists but has NO PK
        var analyzer = new DryRunAnalyzer();
        var reader = new Mock<IStreamReader>();
        SetupReader(reader);

        var writer = new MockKeyValidator
        {
            IsRequired = true,
            RequestedKeys = new List<string> { "id" },
            Dialect = new DtPipe.Core.Dialects.PostgreSqlDialect(),
            TargetPKs = new List<string>() // Empty!
        };
        writer.TargetColumns = new List<ColumnInfo> 
        { 
            new("id", typeof(int), false)
        };

        var result = await analyzer.AnalyzeAsync(reader.Object, new List<IDataTransformer>(), 10, writer);

        Assert.NotNull(result.KeyValidation);
        Assert.True(result.KeyValidation.IsValid);
        Assert.NotNull(result.KeyValidation.Warnings);
        Assert.NotEmpty(result.KeyValidation.Warnings);
        Assert.Contains("Target table has no primary key defined", result.KeyValidation.Warnings![0]);
    }
}
