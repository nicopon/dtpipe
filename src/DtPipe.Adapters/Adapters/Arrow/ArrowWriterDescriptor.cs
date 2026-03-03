using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Arrow;

public class ArrowWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => ArrowMetadata.ComponentName;
    public string Category => "Writer Options";
    public Type OptionsType => typeof(ArrowWriterOptions);
    public bool CanHandle(string connectionString) => ArrowMetadata.CanHandle(connectionString);
    public bool SupportsStdio => ArrowMetadata.SupportsStdio;
    public bool RequiresQuery => false;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (ArrowWriterOptions)options;
        return new ArrowAdapterDataWriter(connectionString, opt);
    }
}
