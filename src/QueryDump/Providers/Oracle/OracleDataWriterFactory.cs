using System.CommandLine;
using System.CommandLine.Parsing;
using QueryDump.Core;
using QueryDump.Configuration;
using QueryDump.Core.Options;
using QueryDump.Cli;

namespace QueryDump.Providers.Oracle;

public class OracleDataWriterFactory : IDataWriterFactory
{
    private readonly OptionsRegistry _registry;

    public OracleDataWriterFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public string Name => "Oracle Writer";
    public string Category => "Writer Options";
    public string SupportedExtension => ""; 

    public bool CanHandle(string outputPath)
    {
        return OracleConnectionHelper.CanHandle(outputPath);
    }

    public IDataWriter Create(DumpOptions options)
    {
        var connectionString = OracleConnectionHelper.GetConnectionString(options.OutputPath);
        return new OracleDataWriter(connectionString, _registry.Get<OracleWriterOptions>());
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return typeof(OracleWriterOptions); 
    }

    private IEnumerable<Option>? _cliOptions;

    public IEnumerable<Option> GetCliOptions()
    {
        return _cliOptions ??= CliOptionBuilder.GenerateOptionsForType(typeof(OracleWriterOptions)).ToList();
    }

    public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
    {
        var options = CliOptionBuilder.BindForType(typeof(OracleWriterOptions), parseResult, GetCliOptions());
        registry.RegisterByType(typeof(OracleWriterOptions), options);
    }
}
