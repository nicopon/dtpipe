using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Configuration;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Parquet;

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
