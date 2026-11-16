using System;
using System.Linq;
using System.Reflection;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// What the MCP says of a stream processor and of a write strategy is read off the component that
/// owns it. These tests hold the catalogue to that: each reads the live registrations, so a
/// processor or an adapter added tomorrow is covered without a line here changing.
/// </summary>
public class McpProcessorHelpTests
{
    private static readonly ComponentCatalog Catalog = ComponentCatalog.Discover(
        typeof(DtPipe.Program).Assembly,
        typeof(DtPipe.Adapters.Csv.CsvReaderDescriptor).Assembly,
        typeof(DtPipe.Processors.Sql.CompositeSqlTransformerFactory).Assembly);

    private static IStreamTransformerFactory[] Processors() => Catalog.StreamTransformers
        .Select(e => (IStreamTransformerFactory)Activator.CreateInstance(e.ImplementationType)!)
        .ToArray();

    private static McpHelpService Help() => new(
        Array.Empty<IStreamReaderFactory>(), Array.Empty<IDataTransformerFactory>(),
        Array.Empty<IDataWriterFactory>(), Processors());

    /// <summary>
    /// The YAML key a refusal and the help name is the factory's <c>ComponentName</c>. A factory
    /// that overrode <c>IsApplicable(JobDefinition)</c> to read another key would make both say
    /// something false, so the convention is checked on every processor registered.
    /// </summary>
    [Fact]
    public void Every_Processor_Is_Selected_By_Its_Advertised_YamlKey()
    {
        foreach (var processor in Processors())
        {
            var job = new JobDefinition
            {
                ProviderOptions = new() { [ProcessorActivation.YamlKey(processor)] = new() },
            };
            Assert.True(processor.IsApplicable(job), $"'{processor.ComponentName}' is not selected by its own key");
        }
    }

    /// <summary>The general help and the listing carry what each factory says of itself, whatever it is.</summary>
    [Fact]
    public void The_Help_Carries_Every_Processor_And_Its_Own_Notes()
    {
        var help = Help().GetGeneralHelp();
        var listed = Help().Processors();

        Assert.Equal(Processors().Length, listed.Count);
        foreach (var p in listed)
        {
            Assert.Contains($"provider-options.{p.YamlKey}", help);
            foreach (var line in p.UsageNotes.Split('\n', StringSplitOptions.RemoveEmptyEntries))
                Assert.Contains(line.TrimEnd('\r'), help);
        }
    }

    /// <summary>
    /// A strategy is described where it is declared. A member without words would leave a caller
    /// choosing by name alone, which is how an overwrite loses data it was meant to keep.
    /// </summary>
    [Fact]
    public void Every_Write_Strategy_Member_Says_What_It_Does()
    {
        var enums = typeof(DtPipe.Adapters.Csv.CsvReaderDescriptor).Assembly.GetTypes()
            .Where(t => t.IsEnum && t.Name.EndsWith("WriteStrategy", StringComparison.Ordinal))
            .ToList();

        Assert.NotEmpty(enums);
        foreach (var type in enums)
            foreach (var name in Enum.GetNames(type))
            {
                var description = type.GetField(name)!.GetCustomAttribute<System.ComponentModel.DescriptionAttribute>();
                Assert.False(string.IsNullOrWhiteSpace(description?.Description), $"{type.Name}.{name} has no description");
            }
    }
}
