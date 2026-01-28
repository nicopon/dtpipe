using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Csv;

public class CsvReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    private const string Prefix = "csv:";

    public string ProviderName => "csv";

    public Type OptionsType => typeof(CsvReaderOptions);

    public bool RequiresQuery => false;

    public bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;
        
        if (connectionString.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return connectionString.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);
    }

    public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        var filePath = connectionString;
        if (filePath.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            filePath = filePath[Prefix.Length..];
        }

        return new CsvStreamReader(filePath, (CsvReaderOptions)options);
    }
}
