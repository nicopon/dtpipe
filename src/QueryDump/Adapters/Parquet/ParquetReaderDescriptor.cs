using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Parquet;

public class ParquetReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    private const string Prefix = "parquet:";

    public string ProviderName => "parquet";

    public Type OptionsType => typeof(EmptyOptions);

    public bool RequiresQuery => false;

    public bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;
        
        if (connectionString.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return connectionString.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase);
    }

    public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        var filePath = connectionString;

        return new ParquetStreamReader(filePath);
    }
}
