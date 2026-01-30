using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;
using DtPipe.Cli;
using DtPipe.Core.Abstractions;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Configuration;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Overwrite;

public interface IOverwriteDataTransformerFactory : IDataTransformerFactory { }

public class OverwriteDataTransformerFactory(OptionsRegistry registry) : IDataTransformerFactory
{
    private readonly OptionsRegistry _registry = registry;

    public static IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<OverwriteDataTransformer>();
    }

    public string Category => "Transformer Options";
    
    public string TransformerType => OverwriteOptions.Prefix; // "overwrite"

    private IEnumerable<Option>? _cliOptions;

    public IEnumerable<Option> GetCliOptions()
    {
        return _cliOptions ??= [.. GetSupportedOptionTypes().SelectMany(CliOptionBuilder.GenerateOptionsForType)];
    }

    public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
    {
        var options = GetCliOptions();

        foreach (var type in GetSupportedOptionTypes())
        {
            var boundOptions = CliOptionBuilder.BindForType(type, parseResult, options);
            registry.RegisterByType(type, boundOptions);
        }
    }

    public IDataTransformer? Create(DumpOptions options)
    {
        var overwriteOptions = _registry.Get<OverwriteOptions>();
        
        if (!overwriteOptions.Overwrite.Any())
        {
            return null;
        }

        return new OverwriteDataTransformer(overwriteOptions);
    }

    public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
    {
        // Get config options (like SkipNull) from registry-bound options
        var registryOptions = _registry.Get<OverwriteOptions>();
        
        var options = new OverwriteOptions
        {
            Overwrite = configuration.Select(x => x.Value),
            SkipNull = registryOptions.SkipNull
        };
        return new OverwriteDataTransformer(options);
    }

    public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
    {
        if (config.Mappings == null || config.Mappings.Count == 0)
            return null;

        // Convert YAML dict to "COLUMN:value" or "COLUMN=value" format
        // If value is empty, just return key (which might already contain the separator like "Col=Val")
        var mappings = config.Mappings.Select(kvp => string.IsNullOrEmpty(kvp.Value) ? kvp.Key : $"{kvp.Key}:{kvp.Value}");
        
        var skipNull = false;
        if (config.Options != null && config.Options.TryGetValue("skip-null", out var snStr))
        {
             bool.TryParse(snStr, out skipNull);
        }

        var options = new OverwriteOptions { Overwrite = mappings, SkipNull = skipNull };
        return new OverwriteDataTransformer(options);
    }
}
