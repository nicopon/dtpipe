using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Parquet;

public class ParquetWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ProviderName => ParquetConstants.ProviderName;

    public Type OptionsType => typeof(ParquetWriterOptions);

    public bool RequiresQuery => false;
    
    public bool CanHandle(string connectionString)
    {
        return connectionString.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase);
    }

    public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        return new ParquetDataWriter(connectionString);
    }
}
