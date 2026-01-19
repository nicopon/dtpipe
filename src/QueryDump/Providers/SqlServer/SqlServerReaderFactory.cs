using System.CommandLine;
using System.CommandLine.Parsing;
using QueryDump.Cli;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.SqlServer;

public class SqlServerReaderFactory : IStreamReaderFactory
{
    private readonly OptionsRegistry _registry;

    public SqlServerReaderFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public string ProviderName => "sqlserver";
    public string Category => "Reader Options";

    public string? ResolveConnectionFromEnvironment() => Environment.GetEnvironmentVariable("MSSQL_CONNECTION_STRING");

    public bool CanHandle(string connectionString)
    {
        return SqlServerConnectionHelper.CanHandle(connectionString);
    }

    public IStreamReader Create(DumpOptions options)
    {
        return new SqlServerStreamReader(
            SqlServerConnectionHelper.GetConnectionString(options.ConnectionString),
            options.Query,
            options.QueryTimeout);
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<SqlServerStreamReader>();
    }

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
}
