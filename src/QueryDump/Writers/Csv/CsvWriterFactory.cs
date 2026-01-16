using System.CommandLine;
using System.CommandLine.Parsing;
using QueryDump.Cli;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Writers.Csv;

namespace QueryDump.Writers.Csv;

public class CsvWriterFactory : IWriterFactory
{
    private readonly OptionsRegistry _registry;

    public CsvWriterFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public string SupportedExtension => ".csv";
    public string Category => "Writer Options";

    public IDataWriter Create(DumpOptions options)
    {
        return new CsvDataWriter(options.OutputPath, _registry.Get<CsvOptions>());
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<CsvDataWriter>();
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
