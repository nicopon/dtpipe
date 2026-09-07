using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using DtPipe.Transformers.Arrow.Fake;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Row.Window;
using DtPipe.Transformers.Services;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The <c>options:</c> half of a YAML transformer block binds against the same properties
/// <c>get-transformer-help</c> prints. Each factory used to read it through a hand-written list of
/// key names instead, and the two drifted: <c>compute-types</c> worked on the command line, bound
/// nothing in YAML, and warned about neither, while an unknown key was dropped without a word.
/// </summary>
[Collection("console-serial")]
public class YamlTransformerBuilderTests
{
    private readonly IJsEngineProvider _js = new JsEngineProvider();

    private static TransformerConfig Config(string type, Dictionary<string, string>? mappings = null, Dictionary<string, string>? options = null)
        => new() { Type = type, Mappings = mappings, Options = options };

    private static void Bind(object options, TransformerConfig config)
        => OptionBinder.BindYaml(options, config.Options!.ToDictionary(kv => kv.Key, kv => (object?)kv.Value), strict: true);

    [Fact]
    public void An_Option_Reachable_From_The_Cli_Is_Reachable_From_Yaml()
    {
        var factory = new ComputeDataTransformerFactory(new OptionsRegistry(), _js);
        var config = Config("compute",
            mappings: new() { ["total"] = "row.a + row.b" },
            options: new() { ["compute-types"] = "total:double" });

        var options = (ComputeOptions)factory.CreateOptionsFromYaml(config)!;
        Bind(options, config);

        Assert.Equal("double", options.ComputeTypes["total"]);
    }

    /// <summary>A comma-separated scalar is the shape the CLI uses; the YAML value arrives
    /// flattened to a string, so it must read the same way.</summary>
    [Fact]
    public void A_Comma_Separated_Value_Binds_To_A_String_Collection()
    {
        var options = new FakeOptions();
        OptionBinder.BindYaml(options, new Dictionary<string, object?> { ["seed-column"] = "id, tenant" }, strict: true);

        Assert.Equal(new[] { "id", "tenant" }, options.SeedColumn);
    }

    [Fact]
    public void An_Unknown_Option_Key_Is_Refused_And_Names_The_Ones_That_Exist()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            OptionBinder.BindYaml(new FakeOptions(), new Dictionary<string, object?> { ["fake-locale"] = "fr" }, strict: true));

        Assert.Contains("fake-locale", ex.Message);
        Assert.Contains("locale", ex.Message);
    }

    /// <summary>
    /// 'deterministic' is not a property, so the generic binder would report it as an unknown key
    /// and suggest nothing — the edit distance to 'seed-row' is far past the threshold. The factory
    /// names the rename before binding, which is what turns a dead job file into a one-line fix.
    /// </summary>
    [Fact]
    public void The_Renamed_Fake_Option_Still_Names_What_Replaced_It()
    {
        var factory = new FakeDataTransformerFactory(new OptionsRegistry());
        var config = Config("fake", options: new() { ["deterministic"] = "true" });

        var ex = Assert.Throws<ArgumentException>(() => factory.CreateOptionsFromYaml(config));
        Assert.Contains("seed-row", ex.Message);
    }

    /// <summary>
    /// A block that sets options and produces no transformer is refused, not skipped. The null
    /// transformer took its columns from 'mappings:', so 'options: {columns: b}' left the column
    /// untouched, exit 0, not a word — the pipeline that ran was not the one that was written.
    /// </summary>
    [Fact]
    public void An_Options_Block_That_Produces_No_Transformer_Is_Refused()
    {
        var factory = new DtPipe.Transformers.Arrow.Null.NullDataTransformerFactory(new OptionsRegistry());
        var config = Config("null", options: new() { ["columns"] = "b" });

        var ex = Assert.Throws<InvalidOperationException>(() => YamlTransformerBuilder.Build(factory, config));

        Assert.Contains("columns", ex.Message);
        Assert.Contains("mappings", ex.Message);
    }

    /// <summary>A block with nothing in it asked for nothing; only a block that sets options and
    /// yields no transformer is a contradiction.</summary>
    [Fact]
    public void An_Empty_Block_Is_Still_Skipped_Quietly()
    {
        var factory = new DtPipe.Transformers.Arrow.Null.NullDataTransformerFactory(new OptionsRegistry());

        Assert.Null(YamlTransformerBuilder.Build(factory, Config("null")));
    }

    /// <summary>An option set in the block wins over what the mappings implied.</summary>
    [Fact]
    public void An_Explicit_Option_Overrides_What_The_Mappings_Encoded()
    {
        var factory = new WindowDataTransformerFactory(_js);
        var config = Config("window",
            mappings: new() { ["ignored"] = "rows" },
            options: new() { ["script"] = "rows.map(r => r)", ["count"] = "5" });

        var options = (WindowOptions)factory.CreateOptionsFromYaml(config)!;
        Bind(options, config);

        Assert.Equal("rows.map(r => r)", options.Script);
        Assert.Equal(5, options.Count);
    }
}
