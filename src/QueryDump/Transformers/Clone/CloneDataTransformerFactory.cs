using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;
using QueryDump.Cli;
using QueryDump.Core;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Clone;

public interface ICloneDataTransformerFactory : ITransformerFactory { }

public class CloneDataTransformerFactory : ICloneDataTransformerFactory
{
    private readonly OptionsRegistry _registry;

    public CloneDataTransformerFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<CloneDataTransformer>();
    }

    public string Category => "Transformer Options";

    private IEnumerable<Option>? _cliOptions;

    public IEnumerable<Option> GetCliOptions()
    {
        return _cliOptions ??= GetSupportedOptionTypes().SelectMany(CliOptionBuilder.GenerateOptionsForType).ToList();
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
        var cloneOptions = _registry.Get<CloneOptions>();
        
        if (!cloneOptions.Mappings.Any())
        {
            return null;
        }

        return new CloneDataTransformer(cloneOptions);
    }
}
