using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Csv;

public class CsvReaderDescriptor : IProviderDescriptor<IStreamReader>
{


    public string ProviderName => "csv";

    public Type OptionsType => typeof(CsvReaderOptions);

    public bool RequiresQuery => false;

    public bool CanHandle(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return false;

        return connectionString.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);
    }

    public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        var filePath = connectionString;

        return new CsvStreamReader(filePath, (CsvReaderOptions)options);
    }
}
