using System.CommandLine;
using System.CommandLine.Parsing;
using QueryDump.Cli;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.Parquet;

public class ParquetReaderFactory : IStreamReaderFactory
{
    private readonly OptionsRegistry _registry;

    public ParquetReaderFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public string ProviderName => "parquet";
    public string Category => "Reader Options";

    private const string Prefix = "parquet:";

    public bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;
        
        if (connectionString.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return connectionString.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase);
    }

    public IStreamReader Create(DumpOptions options)
    {
        var filePath = options.ConnectionString;
        if (filePath.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            filePath = filePath[Prefix.Length..];
        }

        return new ParquetStreamReader(filePath);
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield break; // No specific reader options for Parquet for now
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
