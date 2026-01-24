using System.Data;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Transformers.Script;
using Xunit;

namespace QueryDump.Tests.Integration;

public class ScriptTransformerTests
{
    private readonly ScriptOptions _options;

    public ScriptTransformerTests()
    {
        _options = new ScriptOptions();
    }

    [Fact]
    public async Task Initialize_WithMappings_PreparesProcessors()
    {
        // Arrange
        var options = new ScriptOptions
        {
            Mappings = new[] { "Name:value.substring(0,2)" }
        };
        var transformer = new ScriptDataTransformer(options);
        var columns = new List<ColumnInfo>
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
        var options = new ScriptOptions
        {
            Mappings = new[] { "Name:value.toUpperCase()" }
        };
        var transformer = new ScriptDataTransformer(options);
        var columns = new List<ColumnInfo>
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
        // Jint with strict option usually doesn't persist var across execute calls unless engine reused
        // But here we wrap in a function "function name(value) { ... }" inside Initialize
        // The Engine instance is reused. 
        // But the function scope is local. Globals would be shared?
        // Verify sandbox/no-globals or at least function isolation.
        // Actually, we want to VERIFY parallelism is safe (lock).
        
        var options = new ScriptOptions
        {
            Mappings = new[] { "Val:value * 2" }
        };
        var transformer = new ScriptDataTransformer(options);
        var columns = new[] { new ColumnInfo("Val", typeof(int), false) };
        await transformer.InitializeAsync(columns);

        // Parallel execution
        Parallel.For(0, 100, i => 
        {
            var row = new object?[] { i };
            transformer.Transform(row);
            ((double)row[0]!).Should().Be(i * 2);
        });
    }

    [Fact]
    public async Task Transform_Security_CannotAccessSystem()
    {
        // Require Jint to be in strict mode / sandbox
        var options = new ScriptOptions
        {
            Mappings = new[] { "Val:System.IO.File.Exists('foo')" }
        };
        var transformer = new ScriptDataTransformer(options);
        var columns = new[] { new ColumnInfo("Val", typeof(string), false) };
        await transformer.InitializeAsync(columns);

        var row = new object?[] { "test" };

        // Act & Assert
        // Should throw Jint.Runtime.JavaScriptException or similar, or return undefined/error
        // Because System is not defined in Jint strict mode by default unless CLR allowed.
        
        var act = () => transformer.Transform(row);
        
        // Jint throws JavaScriptException: System is not defined
        act.Should().Throw<Jint.Runtime.JavaScriptException>()
           .WithMessage("*System is not defined*");
    }

    [Fact]
    public async Task Transform_SkipNull_DoesNotExecuteScript()
    {
        // Arrange
        var options = new ScriptOptions
        {
            Mappings = new[] { "Val:value + '_processed'" },
            SkipNull = true
        };
        var transformer = new ScriptDataTransformer(options);
        var columns = new[] { new ColumnInfo("Val", typeof(string), true) };
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
}
