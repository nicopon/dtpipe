using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Arrow;

public class ArrowWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => "arrow";
    public string Category => "Writer Options";
    public Type OptionsType => typeof(ArrowWriterOptions);
    public bool CanHandle(string connectionString) => connectionString.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) || connectionString.EndsWith(".ipc", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => false;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (ArrowWriterOptions)options;
        return new ArrowAdapterDataWriter(connectionString, opt);
    }
}
