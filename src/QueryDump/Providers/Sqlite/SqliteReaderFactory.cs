using System.CommandLine;
using System.CommandLine.Parsing;
using QueryDump.Cli;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.Sqlite;

public class SqliteReaderFactory : IStreamReaderFactory
{
    private readonly OptionsRegistry _registry;

    public SqliteReaderFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public string ProviderName => "sqlite";
    public string Category => "Reader Options";

    public bool CanHandle(string connectionString)
    {
        return SqliteConnectionHelper.CanHandle(connectionString);
    }

    public IStreamReader Create(DumpOptions options)
    {
        var connectionString = SqliteConnectionHelper.ToDataSourceConnectionString(options.ConnectionString);

        return new SqliteStreamReader(
            connectionString,
            options.Query,
            options.QueryTimeout);
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield break; // No specific reader options for SQLite for now
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
