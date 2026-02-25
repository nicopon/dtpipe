using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.XStreamers.Duck;

public class DuckXStreamerFactory : IXStreamerFactory
{
    public string ComponentName => "duck-xstream";
    public string Category => "XStreamers";
    public Type OptionsType => typeof(DuckXStreamerOptions);

    // CRITICAL: Tells the orchestrator to use Arrow channels for this XStreamer
    public XStreamerChannelMode ChannelMode => XStreamerChannelMode.Arrow;

    public bool CanHandle(string connectionString) => false;
    public bool RequiresQuery => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opts = (DuckXStreamerOptions)options;
        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger<DuckXStreamer>();

        var query = opts.Query?.LastOrDefault() ?? "";
        var main = opts.Main?.LastOrDefault() ?? "";
        var refStr = opts.Ref?.LastOrDefault() ?? "";

        var refAliases = string.IsNullOrEmpty(refStr)
            ? Array.Empty<string>()
            : refStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        return new DuckXStreamer(
            registry,
            main,
            refAliases,
            query,
            logger);
    }
}
