using System.CommandLine;
using System.CommandLine.Parsing;
using QueryDump.Cli;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.Sqlite;

public class SqliteDataWriterFactory : IDataWriterFactory
{
    private readonly OptionsRegistry _registry;

    public SqliteDataWriterFactory(OptionsRegistry registry)
    {
        _registry = registry;
        // Register default options
        if (!_registry.Has<SqliteWriterOptions>())
        {
            _registry.Register(new SqliteWriterOptions());
        }
    }

    public string ProviderName => "sqlite";
    public string Category => "Writer Options";
    public string SupportedExtension => ".sqlite";

    public bool CanHandle(string outputPath)
    {
        return SqliteConnectionHelper.CanHandle(outputPath);
    }

    public IDataWriter Create(DumpOptions options)
    {
        var connectionString = SqliteConnectionHelper.ToDataSourceConnectionString(options.OutputPath);
        return new SqliteDataWriter(connectionString, _registry);
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return typeof(SqliteWriterOptions);
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
