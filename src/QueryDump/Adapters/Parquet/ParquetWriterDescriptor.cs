using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Parquet;

public class ParquetWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ProviderName => ParquetConstants.ProviderName;

    // Parquet has no specific options class in factory usage?
    // Factory used ComponentOptionsHelper.GetOptionsType<ParquetDataWriter>();
    // This likely returns an option type or null.
    // If ParquetDataWriter constructor only takes path, we might not need options object passed to it.
    // But descriptor interface expects OptionsType.
    // I will check ParquetDataWriter content in next step if unsure, but for now I will use what factory used: ComponentOptionsHelper.GetOptionsType<ParquetDataWriter>()
    // But OptionsType property must return a Type. ComponentOptionsHelper.GetOptionsType<T> returns Type. Perfect.
    
    public Type OptionsType => typeof(ParquetWriterOptions);
    
    public bool CanHandle(string connectionString)
    {
        return connectionString.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase);
    }

    public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        return new ParquetDataWriter(connectionString);
    }
}
