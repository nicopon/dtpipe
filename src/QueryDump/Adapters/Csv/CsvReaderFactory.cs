using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Csv;

public class CsvReaderFactory : BaseCliContributor, IStreamReaderFactory
{
    private const string Prefix = "csv:";

    public CsvReaderFactory(OptionsRegistry registry) : base(registry)
    {
        // Register default options
        if (!Registry.Has<CsvReaderOptions>())
        {
            Registry.Register(new CsvReaderOptions());
        }
    }

    public override string ProviderName => "csv";
    public override string Category => "Reader Options";

    public bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;
        
        if (connectionString.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return connectionString.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);
    }

    public IStreamReader Create(DumpOptions options)
    {
        var filePath = options.ConnectionString;
        if (filePath.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            filePath = filePath[Prefix.Length..];
        }

        return new CsvStreamReader(filePath, Registry.Get<CsvReaderOptions>());
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return typeof(CsvReaderOptions);
    }
}
