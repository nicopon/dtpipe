using DtPipe.Configuration;
using DtPipe.Transformers.Row.Window;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Row.Expand;
using DtPipe.Transformers.Services;
using DtPipe.Core.Options;
using FluentAssertions;
using Xunit;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Tests.Unit.Configuration;

public class TransformerYamlTests
{
    private readonly IServiceProvider _sp;

    public TransformerYamlTests()
    {
        var services = new ServiceCollection();
        services.AddSingleton<OptionsRegistry>();
        services.AddSingleton<IJsEngineProvider, JsEngineProvider>();
        
        // Register factories
        services.AddSingleton<DtPipe.Core.Abstractions.IDataTransformerFactory, WindowDataTransformerFactory>();
        services.AddSingleton<DtPipe.Core.Abstractions.IDataTransformerFactory, ComputeDataTransformerFactory>();
        services.AddSingleton<DtPipe.Core.Abstractions.IDataTransformerFactory, ExpandDataTransformerFactory>();
        
        _sp = services.BuildServiceProvider();
    }

    [Fact]
    public void WindowTransformer_ShouldLoadFromOptionsScript()
    {
        // Arrange
        var yaml = @"main:
  input: dummy
  transformers:
    - type: window
      options:
        script: 'return rows.map(r => ({ ...r, total: 100 }));'
        count: 10
";
        var tempFile = Path.GetTempFileName();
        File.WriteAllText(tempFile, yaml);

        try
        {
            // Act
            var jobs = JobFileParser.Parse(tempFile);
            var job = jobs["main"];
            var config = job.Transformers!.First();

            var factory = _sp.GetRequiredService<IEnumerable<DtPipe.Core.Abstractions.IDataTransformerFactory>>()
                .OfType<WindowDataTransformerFactory>().First();
            
            var transformer = factory.CreateFromYamlConfig(config) as WindowDataTransformer;

            // Assert
            transformer.Should().NotBeNull();
            var options = typeof(WindowDataTransformer).GetField("_options", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                ?.GetValue(transformer) as WindowOptions;
            
            options.Should().NotBeNull();
            options!.Script.Should().Be("return rows.map(r => ({ ...r, total: 100 }));");
            options.Count.Should().Be(10);
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }

    [Fact]
    public void WindowTransformer_ShouldLoadFromMappingsValues_AsFallback()
    {
        // Arrange
        var yaml = @"main:
  input: dummy
  transformers:
    - type: window
      mappings:
        part1: line1;
        part2: line2;
";
        var tempFile = Path.GetTempFileName();
        File.WriteAllText(tempFile, yaml);

        try
        {
            // Act
            var jobs = JobFileParser.Parse(tempFile);
            var config = jobs["main"].Transformers!.First();
            var factory = _sp.GetRequiredService<IEnumerable<DtPipe.Core.Abstractions.IDataTransformerFactory>>()
                .OfType<WindowDataTransformerFactory>().First();
            
            var transformer = factory.CreateFromYamlConfig(config) as WindowDataTransformer;

            // Assert
            var options = typeof(WindowDataTransformer).GetField("_options", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                ?.GetValue(transformer) as WindowOptions;
            
            options!.Script.Should().Be("line1;\nline2;");
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }

    [Fact]
    public void ComputeTransformer_ShouldLoadFromMappings()
    {
        // Arrange
        var yaml = @"main:
  input: dummy
  transformers:
    - type: compute
      mappings:
        Val: r.Price * 2
";
        var tempFile = Path.GetTempFileName();
        File.WriteAllText(tempFile, yaml);

        try
        {
            // Act
            var jobs = JobFileParser.Parse(tempFile);
            var config = jobs["main"].Transformers!.First();
            var factory = _sp.GetRequiredService<IEnumerable<DtPipe.Core.Abstractions.IDataTransformerFactory>>()
                .OfType<ComputeDataTransformerFactory>().First();
            
            var transformer = factory.CreateFromYamlConfig(config) as ComputeDataTransformer;

            // Assert
            var options = typeof(ComputeDataTransformer).GetField("_options", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                ?.GetValue(transformer) as ComputeOptions;
            
            options!.Compute.Should().Contain("Val:r.Price * 2");
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }
}
