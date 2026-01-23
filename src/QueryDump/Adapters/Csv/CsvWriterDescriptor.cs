using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Csv;

public class CsvWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ProviderName => "csv";

    public Type OptionsType => typeof(CsvOptions);

    public bool CanHandle(string connectionString)
    {
        return connectionString.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);
    }

    public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        return new CsvDataWriter(connectionString, (CsvOptions)options);
    }
}
