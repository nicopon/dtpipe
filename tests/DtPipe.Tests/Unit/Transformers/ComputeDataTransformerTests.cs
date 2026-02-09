using FluentAssertions;
using DtPipe.Core.Models;
using DtPipe.Transformers.Script;
using Xunit;
using DtPipe.Core.Services;

namespace DtPipe.Tests.Unit.Transformers;

public class ComputeDataTransformerTests : IDisposable
{
    private readonly ComputeOptions _options;
    private readonly IJsEngineProvider _jsEngineProvider;

    public ComputeDataTransformerTests()
    {
        _options = new ComputeOptions();
        _jsEngineProvider = new JsEngineProvider();
    }
    
    public void Dispose()
    {
        _jsEngineProvider?.Dispose();
    }

    [Fact]
    public async Task Initialize_WithScript_PreparesProcessors()
    {
        // Arrange
        var options = new ComputeOptions
        {
            Compute = new[] { "Name:row.Name.substring(0,2)" }
        };
        var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
        var columns = new List<PipeColumnInfo>
        {
            new("Name", typeof(string), false)
        };

        // Act
        var result = await transformer.InitializeAsync(columns);

        // Assert
        result.Should().BeEquivalentTo(columns);
    }
    
    [Fact]
    public async Task Transform_ModifiesValue_UsingScript()
    {
        // Arrange
        var options = new ComputeOptions
        {
            Compute = new[] { "Name:row.Name.toUpperCase()" }
        };
        var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
        var columns = new List<PipeColumnInfo>
        {
            new("Id", typeof(int), false),
            new("Name", typeof(string), false)
        };
        
        await transformer.InitializeAsync(columns);
        var row = new object?[] { 1, "Alice" };

        // Act
        transformer.Transform(row);

        // Assert
        row[1].Should().Be("ALICE");
    }

    [Fact]
    public async Task Transform_SequentialExecution_MaintainsState_IfScriptAllows()
    {
        // Verify parallelism is safe with engine locking (handled by provider via ThreadLocal or new instance per thread)
        
        var options = new ComputeOptions
        {
            Compute = new[] { "Val:row.Val * 2" }
        };
        // Note: Transformer instance is shared across threads? 
        // If parallelism happens, Transform is called concurrently.
        // JsEngineProvider uses ThreadLocal, so each thread gets its own engine.
        
        var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
        var columns = new[] { new PipeColumnInfo("Val", typeof(int), false) };
        await transformer.InitializeAsync(columns);

        // Sequential execution
        for (int i = 0; i < 100; i++) 
        {
            var row = new object?[] { i };
            transformer.Transform(row);
            ((double)row[0]!).Should().Be(i * 2);
        }
    }

    [Fact]
    public async Task Transform_Security_CannotAccessSystem()
    {
        // Require Jint to be in strict mode / sandbox
        var options = new ComputeOptions
        {
            Compute = new[] { "Val:System.IO.File.Exists('foo')" }
        };
        var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
        var columns = new[] { new PipeColumnInfo("Val", typeof(string), false) };
        await transformer.InitializeAsync(columns);

        var row = new object?[] { "test" };

        // Act & Assert
        // Should throw Jint.Runtime.JavaScriptException or similar, or return undefined/error
        // Because System is not defined in Jint strict mode by default unless CLR allowed.
        
        var act = () => transformer.Transform(row);
        
        // Changed: ComputeDataTransformer now wraps execution errors in InvalidOperationException
        act.Should().Throw<InvalidOperationException>()
           .WithInnerException<Jint.Runtime.JavaScriptException>();
    }

    [Fact]
    public async Task Transform_SkipNull_DoesNotExecuteScript()
    {
        // Arrange
        var options = new ComputeOptions
        {
            Compute = new[] { "Val:row.Val + '_processed'" },
            SkipNull = true
        };
        var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
        var columns = new[] { new PipeColumnInfo("Val", typeof(string), true) };
        await transformer.InitializeAsync(columns);

        var row = new object?[] { null };

        // Act
        transformer.Transform(row);

        // Assert
        row[0].Should().BeNull(); // Should remain null
        
        // Safety check: if it ran, it would likely ideally throw or return "null_processed" (if Jint handles null as null value in JS)
        // Let's verify with non-null matches
        
        var row2 = new object?[] { "test" };
        transformer.Transform(row2);
        row2[0].Should().Be("test_processed");
    }

    [Fact]
    public async Task Transform_CanAccessOtherColumns_UsingRow()
    {
        // Arrange
        var options = new ComputeOptions
        {
            Compute = new[] { "FullName:return row.FirstName + ' ' + row.LastName;" }
        };
        var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
        var columns = new List<PipeColumnInfo>
        {
            new("FirstName", typeof(string), false),
            new("LastName", typeof(string), false),
            new("FullName", typeof(string), false)
        };
        
        await transformer.InitializeAsync(columns);
        var row = new object?[] { "Alice", "Smith", "" };

        // Act
        transformer.Transform(row);

        // Assert
        row[2].Should().Be("Alice Smith");
    }

    [Fact]
    public async Task Transform_CanAccessColumns_UsingDictionarySyntax()
    {
        // Arrange: Handle columns with spaces using row["Col Name"]
        var options = new ComputeOptions
        {
            Compute = new[] { "Code:row['Product Code']" }
        };
        var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
        var columns = new List<PipeColumnInfo>
        {
            new("Product Code", typeof(string), false),
            new("Code", typeof(string), false)
        };
        
        await transformer.InitializeAsync(columns);
        var row = new object?[] { "ABC-123", "" };

        // Act
        transformer.Transform(row);

        // Assert
        row[1].Should().Be("ABC-123");
    }
}
