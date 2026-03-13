using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Abstractions.Dag;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.XStreamers.DataFusion;

/// <summary>
/// CLI options for the fusion-engine component.
/// Two modes:
///   A) Local sources: --src-main and --src-ref pass the path, DataFusion reads directly.
///   B) Upstream DAG sources: --main and --ref read from Arrow memory channels.
/// </summary>
public class DataFusionXStreamerOptions : IOptionSet, ICliOptionMetadata
{
    public static string Prefix => "fusion-engine";
    public static string DisplayName => "DataFusion In-Process SQL Engine";

    /// <summary>SQL query to execute.</summary>
    public string? Query { get; set; }

    /// <summary>Alias of the main branch in the upstream DAG (memory channel mode).</summary>
    public string? MainAlias { get; set; }

    /// <summary>Alias of the ref branches in the upstream DAG (memory channel mode).</summary>
    public string[] RefAlias { get; set; } = Array.Empty<string>();

    /// <summary>
    /// Direct source for the main stream (e.g., parquet:/path/main.parquet or csv:/path/main.csv).
    /// If present, DataFusion reads the file directly — bypassing the memory channel.
    /// </summary>
    public string? SrcMain { get; set; }

    /// <summary>
    /// Direct source(s) for the ref streams (e.g., csv:/path/ref.csv).
    /// Each entry corresponds to a ref alias (in the order of RefAlias).
    /// </summary>
    public string[] SrcRef { get; set; } = Array.Empty<string>();

    public IReadOnlyDictionary<string, string> PropertyToFlag => new Dictionary<string, string>
    {
        { nameof(Query), "--query" },
        { nameof(MainAlias), "--main" },
        { nameof(RefAlias), "--ref" },
        { nameof(SrcMain), "--src-main" },
        { nameof(SrcRef), "--src-ref" }
    };
}

public class DataFusionXStreamerFactory : IXStreamerFactory
{
    private readonly OptionsRegistry? _registry;

    public DataFusionXStreamerFactory() { }
    public DataFusionXStreamerFactory(OptionsRegistry registry) { _registry = registry; }

    public string ComponentName => "fusion-engine";
    public string Category => "XStreamers";
    public Type OptionsType => typeof(DataFusionXStreamerOptions);
    public bool RequiresQuery => true;
    public bool CanHandle(string connectionString) => false;

    // Requires upstream Arrow RecordBatch channels (like duck-engine)
    public ChannelMode ChannelMode => ChannelMode.Arrow;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opts = (DataFusionXStreamerOptions)options;
        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();

        return new DataFusionXStreamer(
            registry: registry,
            query: opts.Query ?? throw new ArgumentException("--query is required"),
            mainAlias: opts.MainAlias ?? "",
            refAliases: opts.RefAlias ?? Array.Empty<string>(),
            srcMain: opts.SrcMain ?? "",
            srcRefs: opts.SrcRef ?? Array.Empty<string>(),
            logger: loggerFactory.CreateLogger<DataFusionXStreamer>()
        );
    }
}
