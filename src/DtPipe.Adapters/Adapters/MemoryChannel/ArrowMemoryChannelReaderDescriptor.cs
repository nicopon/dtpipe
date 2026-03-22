using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Adapters.MemoryChannel;

public class ArrowMemoryChannelReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => ArrowMemoryChannelMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(ArrowMemoryChannelOptions);
    public bool CanHandle(string connectionString) => ArrowMemoryChannelMetadata.CanHandle(connectionString);
    public bool SupportsStdio => ArrowMemoryChannelMetadata.SupportsStdio;
    public bool RequiresQuery => false;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var entry = registry.GetArrowChannel(connectionString)
            ?? throw new InvalidOperationException($"Arrow memory channel '{connectionString}' not found in registry.");
        return new ArrowMemoryChannelStreamReader(entry.Channel.Reader, registry, connectionString);
    }
}
