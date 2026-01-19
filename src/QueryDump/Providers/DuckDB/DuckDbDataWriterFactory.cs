using System.CommandLine;
using System.CommandLine.Parsing;
using QueryDump.Core;
using QueryDump.Configuration;
using QueryDump.Core.Options;
using QueryDump.Cli;

namespace QueryDump.Providers.DuckDB;

public class DuckDbDataWriterFactory : IDataWriterFactory
{
    private readonly OptionsRegistry _registry;

    public DuckDbDataWriterFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public string Name => "DuckDB Writer";
    public string Category => "Writer Options";
    public string SupportedExtension => ".duckdb"; 

    public bool CanHandle(string outputPath)
    {
        return DuckDbConnectionHelper.CanHandle(outputPath);
    }

    public IDataWriter Create(DumpOptions options)
    {
        var connectionString = DuckDbConnectionHelper.GetConnectionString(options.OutputPath);
        
        if (!connectionString.Contains("DataSource=", StringComparison.OrdinalIgnoreCase) 
            && !connectionString.StartsWith("Data Source=", StringComparison.OrdinalIgnoreCase))
        {
            connectionString = $"Data Source={connectionString}";
        }

        return new DuckDbDataWriter(connectionString, _registry.Get<DuckDbWriterOptions>());
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return typeof(DuckDbWriterOptions); 
    }

    private IEnumerable<Option>? _cliOptions;

    public IEnumerable<Option> GetCliOptions()
    {
        return _cliOptions ??= CliOptionBuilder.GenerateOptionsForType(typeof(DuckDbWriterOptions)).ToList();
    }

    public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
    {
        var options = CliOptionBuilder.BindForType(typeof(DuckDbWriterOptions), parseResult, GetCliOptions());
        registry.RegisterByType(typeof(DuckDbWriterOptions), options);
    }
}
