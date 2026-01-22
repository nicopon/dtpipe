using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Parquet;

public class ParquetReaderFactory : BaseCliContributor, IStreamReaderFactory
{
    private const string Prefix = "parquet:";

    public ParquetReaderFactory(OptionsRegistry registry) : base(registry) { }

    public override string ProviderName => "parquet";
    public override string Category => "Reader Options";

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

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield break; // No specific reader options for Parquet
    }
}
