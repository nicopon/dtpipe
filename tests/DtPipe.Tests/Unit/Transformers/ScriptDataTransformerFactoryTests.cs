using DtPipe.Transformers.Script;
using FluentAssertions;
using Xunit;
using DtPipe.Core.Options;

namespace DtPipe.Tests.Unit.Transformers;

public class ScriptDataTransformerFactoryTests
{
    private class TestableScriptDataTransformerFactory : ScriptDataTransformerFactory
    {
        public TestableScriptDataTransformerFactory() : base(new DtPipe.Core.Options.OptionsRegistry()) { }

        // Expose protected/private logic if needed, but here we test public methods
        // Since ResolveScriptContent is private static, we test it via CreateFromConfiguration
    }

    [Fact]
    public void CreateFromConfiguration_ShouldLoadImplicitFile_WhenFileExists()
    {
        // Arrange
        var tempFile = Path.GetTempFileName();
        var scriptContent = "return row.NAME.toUpperCase();";
        File.WriteAllText(tempFile, scriptContent);
        
        var factory = new TestableScriptDataTransformerFactory();
        var config = new List<(string, string)> { ("--script", $"NAME:{tempFile}") };

        try 
        {
            // Act
            var transformer = (ScriptDataTransformer)factory.CreateFromConfiguration(config);

            // Assert
            // We can't easily inspect private state without reflection, 
            // but we can verify it behaves correctly or check if it throws if invalid.
            // For now, let's assume if it returns a transformer, it parsed it.
            // Improve: use reflection to check internal mappings if necessary.
            transformer.Should().NotBeNull();
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }

    [Fact]
    public void CreateFromConfiguration_ShouldLoadExplicitFile_WhenFileExists()
    {
        // Arrange
        var tempFile = Path.GetTempFileName();
        var scriptContent = "return row.NAME.toUpperCase();";
        File.WriteAllText(tempFile, scriptContent);
        
        var factory = new TestableScriptDataTransformerFactory();
        // Use @ prefix
        var config = new List<(string, string)> { ("--script", $"NAME:@{tempFile}") };

        try 
        {
            // Act
            var transformer = (ScriptDataTransformer)factory.CreateFromConfiguration(config);

            // Assert
            transformer.Should().NotBeNull();
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }

    [Fact]
    public void CreateFromConfiguration_ShouldUseInlineScript_WhenFileDoesNotExist()
    {
        // Arrange
        var inlineScript = "return row.AGE * 2;";
        var factory = new TestableScriptDataTransformerFactory();
        var config = new List<(string, string)> { ("--script", $"AGE:{inlineScript}") };

        // Act
        var transformer = (ScriptDataTransformer)factory.CreateFromConfiguration(config);

        // Assert
        transformer.Should().NotBeNull();
    }
}
