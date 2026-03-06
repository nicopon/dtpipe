using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.MemoryChannel;

public class ArrowMemoryChannelWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => ArrowMemoryChannelMetadata.ComponentName;
    public string Category => "Writer Options";
    public Type OptionsType => typeof(ArrowMemoryChannelOptions);
    public bool RequiresQuery => false;

    public bool CanHandle(string connectionString) => ArrowMemoryChannelMetadata.CanHandle(connectionString);
    public bool SupportsStdio => ArrowMemoryChannelMetadata.SupportsStdio;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opts = (ArrowMemoryChannelOptions)options;
        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger<ArrowMemoryChannelDataWriter>();

        var alias = connectionString; // The orchestrator passes the alias as the connection string

        var channelData = registry.GetArrowChannel(alias) ??
            throw new InvalidOperationException($"Arrow channel '{alias}' not found in registry. Ensure it was registered by the orchestrator.");

        return new ArrowMemoryChannelDataWriter(
            channelData.Channel.Writer,
            registry,
            alias,
            opts.BatchSize,
            logger);
    }
}
