using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;
using QueryDump.Cli;
using QueryDump.Core;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Format;

public interface IFormatDataTransformerFactory : IDataTransformerFactory { }

public class FormatDataTransformerFactory(OptionsRegistry registry) : IDataTransformerFactory
{
    private readonly OptionsRegistry _registry = registry;

    public static IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<FormatDataTransformer>();
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
        var formatOptions = _registry.Get<FormatOptions>();
        
        if (!formatOptions.Mappings.Any())
        {
            return null;
        }

        return new FormatDataTransformer(formatOptions);
    }

    public IDataTransformer CreateFromConfiguration(IEnumerable<string> values)
    {
        var options = new FormatOptions
        {
            Mappings = values
        };
        return new FormatDataTransformer(options);
    }
}
