using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Content of <c>get-transformer-help</c> over the real transformer catalog. What a model reads
/// here is the only description it gets of a transformer's YAML surface, so the listing must match
/// what the loader accepts.
/// </summary>
public class McpTransformerHelpTests
{
    private readonly IMcpHelpService _help;
    private readonly IReadOnlyList<IDataTransformerFactory> _factories;

    public McpTransformerHelpTests()
    {
        var registry = new OptionsRegistry();
        var js = new JsEngineProvider();
        object?[][] shapes = [[], [registry], [registry, js], [registry, js, null], [js]];

        _factories = typeof(DtPipe.Transformers.Arrow.Fake.FakeDataTransformerFactory).Assembly
            .GetTypes()
            .Where(t => !t.IsAbstract && typeof(IDataTransformerFactory).IsAssignableFrom(t))
            .Select(t => shapes.Select(shape => TryCreate(t, shape)).FirstOrDefault(f => f is not null))
            .OfType<IDataTransformerFactory>()
            .ToList();

        _help = new McpHelpService(Array.Empty<IStreamReaderFactory>(), _factories, Array.Empty<IDataWriterFactory>());
    }

    private static IDataTransformerFactory? TryCreate(Type type, object?[] shape)
    {
        try { return Activator.CreateInstance(type, shape) as IDataTransformerFactory; }
        catch { return null; }
    }

    public static IEnumerable<object[]> Transformers()
        => new McpTransformerHelpTests()._factories.Select(f => new object[] { f.ComponentName });

    /// <summary>
    /// What <c>mappings:</c> encodes is not an <c>options:</c> key. Setting it there binds but
    /// changes nothing: the factory decides it has no work before the options block is read. The
    /// null transformer listed its 'columns' that way — the help advertised a key that silently did
    /// nothing while its own usage notes told you to use mappings.
    /// </summary>
    [Theory]
    [MemberData(nameof(Transformers))]
    public void The_Mapping_Property_Is_Not_Listed_As_An_Option(string transformer)
    {
        var factory = _factories.First(f => f.ComponentName.Equals(transformer, StringComparison.OrdinalIgnoreCase));
        var primary = OptionObjectExporter.PrimaryMappingPropertyFor(factory.OptionsType);
        if (primary is null) return;

        Assert.DoesNotContain($"\n  {primary.ToKebabCase()}: ", _help.GetTransformerHelp(transformer));
    }

    /// <summary>
    /// A value 'fake' does not recognise is refused. Accepting it as a constant is what let a
    /// recorded session map four columns to 'firstName', 'lastName', 'safeEmail' and 'membership'
    /// and ship two thousand rows of those four words as anonymised data.
    /// </summary>
    [Fact]
    public void Fake_Says_That_An_Unknown_Value_Is_Refused()
    {
        var help = _help.GetTransformerHelp("fake");

        Assert.Contains("refused", help, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("name.firstName", help);
    }

    /// <summary>
    /// project has no mappings-encoded property, so all three of its options are real and reachable
    /// — `--export-job` emits exactly this shape. Keying the exclusion on the component name hid
    /// 'project' from its own help.
    /// </summary>
    [Fact]
    public void Project_Publishes_The_Options_That_Work()
    {
        var help = _help.GetTransformerHelp("project");

        Assert.Contains("project: <value>", help);
        Assert.Contains("drop: <value>", help);
        Assert.Contains("rename: <value>", help);
    }
}
