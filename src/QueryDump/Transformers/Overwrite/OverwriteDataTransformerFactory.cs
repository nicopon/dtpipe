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

    public IDataTransformer CreateFromConfiguration(IEnumerable<string> values)
    {
        var options = new OverwriteOptions
        {
            Mappings = [.. values]
        };
        return new OverwriteDataTransformer(options);
    }
}
