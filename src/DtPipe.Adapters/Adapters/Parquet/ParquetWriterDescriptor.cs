using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Parquet;

public class ParquetWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => ParquetMetadata.ComponentName;
    public string Category => "Writer Options";
    public Type OptionsType => typeof(ParquetWriterOptions);
    public bool CanHandle(string connectionString) => ParquetMetadata.CanHandle(connectionString);
    public bool SupportsStdio => ParquetMetadata.SupportsStdio;
    public bool RequiresQuery => false;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (ParquetWriterOptions)options;
        return new ParquetDataWriter(connectionString);
    }
}
