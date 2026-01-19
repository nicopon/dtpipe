using System.CommandLine;
using System.CommandLine.Parsing;
using QueryDump.Cli;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.DuckDB;

public class DuckDbReaderFactory : IStreamReaderFactory
{
    private readonly OptionsRegistry _registry;

    public DuckDbReaderFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public string ProviderName => "duckdb";
    public string Category => "Reader Options";

    public string? ResolveConnectionFromEnvironment() => Environment.GetEnvironmentVariable("DUCKDB_CONNECTION_STRING");

    public bool CanHandle(string connectionString)
    {
        // DuckDB often just takes a file path or usage of specific keys
        return DuckDbConnectionHelper.CanHandle(connectionString);
    }

    public IStreamReader Create(DumpOptions options)
    {
        var connectionString = DuckDbConnectionHelper.GetConnectionString(options.ConnectionString);

        if (!connectionString.Contains("DataSource=", StringComparison.OrdinalIgnoreCase) 
            && !connectionString.StartsWith("Data Source=", StringComparison.OrdinalIgnoreCase))
        {
            connectionString = $"Data Source={connectionString}";
        }

        return new DuckDataSourceReader(
            connectionString,
            options.Query,
            _registry.Get<DuckDbOptions>(),
            options.QueryTimeout);
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<DuckDataSourceReader>();
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
