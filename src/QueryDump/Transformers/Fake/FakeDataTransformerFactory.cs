using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;
using QueryDump.Cli;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Fake;

/// <summary>
/// Factory for creating fake data transformers.
/// </summary>
public interface IFakeDataTransformerFactory : ITransformerFactory
{
}

public class FakeDataTransformerFactory : IFakeDataTransformerFactory
{
    private readonly OptionsRegistry _registry;

    public FakeDataTransformerFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<FakeDataTransformer>();
    }

    public string Category => "Transformer Options";

    private IEnumerable<Option>? _cliOptions;

    public IEnumerable<Option> GetCliOptions()
    {
        if (_cliOptions != null) return _cliOptions;

        var list = new List<Option>();

        // Manual option for listing fakers (not bound to POCO yet)
        var fakeListOption = new Option<bool>("--fake-list")
        {
            Description = "List all available fake data generators and exit"
        };
        list.Add(fakeListOption);

        foreach (var type in GetSupportedOptionTypes())
        {
            list.AddRange(CliOptionBuilder.GenerateOptionsForType(type));
        }

        return _cliOptions = list;
    }

    public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
    {
        var allOptions = GetCliOptions();

        foreach (var type in GetSupportedOptionTypes())
        {
            var boundOptions = CliOptionBuilder.BindForType(type, parseResult, allOptions);
            registry.RegisterByType(type, boundOptions);
        }
    }

    public IDataTransformer? Create(DumpOptions options)
    {
        var fakeOptions = _registry.Get<FakeOptions>();

        if (fakeOptions.Mappings.Count == 0)
        {
            return null;
        }

        return new FakeDataTransformer(fakeOptions);
    }
}
