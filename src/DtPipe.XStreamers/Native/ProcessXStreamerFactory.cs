using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.XStreamers.Native;

public class ProcessXStreamerFactory : IXStreamerFactory
{
    public string ComponentName => "duck-engine";
    public string Category => "XStreamers";
    public Type OptionsType => typeof(ProcessXStreamerOptions);

    // ProcessXStreamer consumes Arrow RecordBatch channels directly and pipes them to the engine
    public XStreamerChannelMode ChannelMode => XStreamerChannelMode.Arrow;

    public bool RequiresQuery => true;

    public bool CanHandle(string connectionString) => false;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opts = (ProcessXStreamerOptions)options;
        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger<ProcessXStreamer>();

        return new ProcessXStreamer(
            registry,
            opts.Query?.LastOrDefault() ?? "",
            opts.MainAlias?.LastOrDefault() ?? "",
            opts.RefAlias ?? Array.Empty<string>(),
            logger,
            // Piste D: direct source specs bypass memory channels entirely
            srcMain: opts.SrcMain?.LastOrDefault() ?? "",
            srcRefs: opts.SrcRef ?? Array.Empty<string>()
        );
    }
}

public class ProcessXStreamerOptions : IOptionSet, ICliOptionMetadata
{
    public static string Prefix => "duck-engine";
    public static string DisplayName => "DuckDB Process Orchestrator";

    public string[] Query { get; set; } = Array.Empty<string>();

    /// <summary>Alias in memory channel (legacy mode, reads from DAG upstream branches).</summary>
    public string[] MainAlias { get; set; } = Array.Empty<string>();

    /// <summary>Alias(es) in memory channel (legacy mode).</summary>
    public string[] RefAlias { get; set; } = Array.Empty<string>();

    /// <summary>Piste D: direct source spec for main stream (e.g. parquet:/path/main.parquet). Spawns a dtpipe subprocess.</summary>
    public string[] SrcMain { get; set; } = Array.Empty<string>();

    /// <summary>Piste D: direct source spec(s) for ref stream(s) (e.g. csv:/path/ref.csv). Each spawns a dtpipe subprocess.</summary>
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
