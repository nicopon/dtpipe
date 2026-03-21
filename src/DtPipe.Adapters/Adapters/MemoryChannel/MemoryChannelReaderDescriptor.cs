using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Adapters.MemoryChannel;

public class MemoryChannelReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => MemoryChannelMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(MemoryChannelOptions);
    public bool CanHandle(string connectionString) => MemoryChannelMetadata.CanHandle(connectionString);
    public bool SupportsStdio => MemoryChannelMetadata.SupportsStdio;
    public bool RequiresQuery => false;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var entry = registry.GetChannel(connectionString);
        if (entry == null) throw new InvalidOperationException($"Memory channel '{connectionString}' not found in registry.");

        // Pass registry + alias so that OpenAsync can wait for the schema to be published
        // by the producing branch (needed for fan-out sub-channels registered with empty schema).
        return new MemoryChannelStreamReader(entry.Value.Channel.Reader, registry, connectionString);
    }
}
