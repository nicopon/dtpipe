using DtPipe.Core.Options;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.XStreamers.DataFusion;

/// <summary>
/// Options CLI pour le composant fusion-engine.
/// Deux modes :
///   A) Sources locales : --src-main et --src-ref passent le chemin, DataFusion lit directement.
///   B) Sources upstream DAG : --main et --ref lisent depuis les Arrow memory channels.
/// </summary>
public class DataFusionXStreamerOptions : IOptionSet, ICliOptionMetadata
{
    public static string Prefix => "fusion-engine";
    public static string DisplayName => "DataFusion In-Process SQL Engine";

    /// <summary>Requête SQL à exécuter.</summary>
    public string[] Query { get; set; } = Array.Empty<string>();

    /// <summary>Alias de la branche main dans le DAG upstream (mode memory channel).</summary>
    public string[] MainAlias { get; set; } = Array.Empty<string>();

    /// <summary>Alias des branches ref dans le DAG upstream (mode memory channel).</summary>
    public string[] RefAlias { get; set; } = Array.Empty<string>();

    /// <summary>
    /// Source directe pour le flux main (ex: parquet:/path/main.parquet ou csv:/path/main.csv).
    /// Si présent, DataFusion lit directement le fichier — bypasse le memory channel.
    /// </summary>
    public string[] SrcMain { get; set; } = Array.Empty<string>();

    /// <summary>
    /// Source(s) directe(s) pour les flux ref (ex: csv:/path/ref.csv).
    /// Chaque entrée correspond à un alias ref (dans l'ordre de RefAlias).
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
    public string ComponentName => "fusion-engine";
    public string Category => "XStreamers";
    public Type OptionsType => typeof(DataFusionXStreamerOptions);
    public bool RequiresQuery => true;
    public bool CanHandle(string connectionString) => false;

    // Requiert des Arrow RecordBatch channels upstream (comme duck-engine)
    public XStreamerChannelMode ChannelMode => XStreamerChannelMode.Arrow;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opts = (DataFusionXStreamerOptions)options;
        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();

        return new DataFusionXStreamer(
            registry: registry,
            query: opts.Query?.LastOrDefault() ?? throw new ArgumentException("--query is required"),
            mainAlias: opts.MainAlias?.LastOrDefault() ?? "",
            refAliases: opts.RefAlias ?? Array.Empty<string>(),
            srcMain: opts.SrcMain?.LastOrDefault() ?? "",
            srcRefs: opts.SrcRef ?? Array.Empty<string>(),
            logger: loggerFactory.CreateLogger<DataFusionXStreamer>()
        );
    }
}
