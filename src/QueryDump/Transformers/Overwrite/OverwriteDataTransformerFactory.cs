using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;
using QueryDump.Cli;
using QueryDump.Core;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Overwrite;

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
        
        if (!overwriteOptions.Mappings.Any())
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
            Mappings = [.. configuration.Select(x => x.Value)],
            SkipNull = registryOptions.SkipNull
        };
        return new OverwriteDataTransformer(options);
    }

    public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
    {
        if (config.Mappings == null || config.Mappings.Count == 0)
            return null;

        // Convert YAML dict to "COLUMN:value" format
        var mappings = config.Mappings.Select(kvp => $"{kvp.Key}:{kvp.Value}");
        
        var options = new OverwriteOptions { Mappings = [.. mappings] };
        return new OverwriteDataTransformer(options);
    }
}
