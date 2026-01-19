using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;
using QueryDump.Cli;
using QueryDump.Core;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Null;

public interface INullDataTransformerFactory : IDataTransformerFactory { }

public class NullDataTransformerFactory(OptionsRegistry registry) : IDataTransformerFactory
{
    private readonly OptionsRegistry _registry = registry;

    public static IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<NullDataTransformer>();
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
        var nullOptions = _registry.Get<NullOptions>();
        
        // Return null if no columns to nullify, to skip execution overhead
        if (!nullOptions.Columns.Any())
        {
            return null;
        }

        return new NullDataTransformer(nullOptions);
    }

    public IDataTransformer CreateFromConfiguration(IEnumerable<string> values)
    {
        var options = new NullOptions
        {
            Columns = [.. values]
        };
        return new NullDataTransformer(options);
    }
}
