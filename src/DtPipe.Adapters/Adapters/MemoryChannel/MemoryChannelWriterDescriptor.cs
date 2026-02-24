using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.MemoryChannel;

public class MemoryChannelWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => "memory";
    public Type OptionsType => typeof(MemoryChannelOptions);
    public string Category => "Writers";

    public bool CanHandle(string connectionString) => false;
    public bool RequiresQuery => false;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var entry = registry.GetChannel(connectionString);
        if (entry == null) throw new InvalidOperationException($"Memory channel '{connectionString}' not found in registry.");

        var logger = serviceProvider.GetRequiredService<ILogger<MemoryChannelDataWriter>>();
        return new MemoryChannelDataWriter(entry.Value.Channel.Writer, registry, connectionString, logger);
    }
}
