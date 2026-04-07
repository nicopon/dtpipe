using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;
using Apache.Arrow;

namespace DtPipe.Adapters.MemoryChannel;

public class MemoryChannelWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => MemoryChannelMetadata.ComponentName;
    public string Category => "Writer Options";
    public Type OptionsType => typeof(MemoryChannelOptions);
    public bool CanHandle(string connectionString) => MemoryChannelMetadata.CanHandle(connectionString);
    public bool SupportsStdio => MemoryChannelMetadata.SupportsStdio;
    public bool RequiresQuery => false;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var entry = registry.GetArrowChannel(connectionString);
        if (entry == null) throw new InvalidOperationException($"Arrow Memory channel '{connectionString}' not found in registry.");

        var logger = serviceProvider.GetRequiredService<ILogger<MemoryChannelDataWriter>>();
        return new MemoryChannelDataWriter(entry.Value.Channel.Writer, registry, connectionString, logger);
    }
}
