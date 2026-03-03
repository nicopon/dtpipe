using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Csv;

public class CsvReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => CsvMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(CsvReaderOptions);
    public bool CanHandle(string connectionString) => CsvMetadata.CanHandle(connectionString);
    public bool SupportsStdio => CsvMetadata.SupportsStdio;
    public bool RequiresQuery => false;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (CsvReaderOptions)options;
        return new CsvStreamReader(connectionString, opt, serviceProvider.GetRequiredService<ILogger<CsvStreamReader>>());
    }
}
