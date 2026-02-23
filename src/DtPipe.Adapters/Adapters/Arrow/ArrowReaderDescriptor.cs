using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Arrow;

public class ArrowReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => "arrow";
    public string Category => "Reader Options";
    public Type OptionsType => typeof(ArrowReaderOptions);
    public bool CanHandle(string connectionString) => connectionString.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) || connectionString.EndsWith(".ipc", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => false;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (ArrowReaderOptions)options;
        return new ArrowAdapterStreamReader(connectionString, opt, serviceProvider.GetRequiredService<ILogger<ArrowAdapterStreamReader>>());
    }
}
