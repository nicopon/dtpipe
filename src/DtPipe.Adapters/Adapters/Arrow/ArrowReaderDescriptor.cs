using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Arrow;

public class ArrowReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => ArrowMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(ArrowReaderOptions);
    public bool CanHandle(string connectionString) => ArrowMetadata.CanHandle(connectionString);
    public bool SupportsStdio => ArrowMetadata.SupportsStdio;
    public bool RequiresQuery => false;
    public bool YieldsColumnarOutput => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (ArrowReaderOptions)options;
        return new ArrowAdapterStreamReader(connectionString, opt, serviceProvider.GetRequiredService<ILogger<ArrowAdapterStreamReader>>());
    }
}
