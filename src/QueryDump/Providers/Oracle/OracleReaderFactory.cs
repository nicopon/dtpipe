using System.CommandLine;
using System.CommandLine.Parsing;
using QueryDump.Cli;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.Oracle;

public class OracleReaderFactory : IReaderFactory
{
    private readonly OptionsRegistry _registry;

    public OracleReaderFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public string ProviderName => "oracle";
    public string Category => "Reader Options";

    public IStreamReader Create(DumpOptions options)
    {
        return new OracleStreamReader(
            options.ConnectionString, 
            options.Query,
            _registry.Get<OracleOptions>(),
            options.QueryTimeout);
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<OracleStreamReader>();
    }

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
}
