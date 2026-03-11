using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Parquet;

public class ParquetReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => ParquetMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(ParquetReaderOptions);
    public bool CanHandle(string connectionString) => ParquetMetadata.CanHandle(connectionString);
    public bool SupportsStdio => ParquetMetadata.SupportsStdio;
    public bool RequiresQuery => false;
    public bool YieldsColumnarOutput => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (ParquetReaderOptions)options;
        return new ParquetStreamReader(connectionString, serviceProvider.GetRequiredService<ILogger<ParquetStreamReader>>());
    }
}
