using DtPipe.Core.Abstractions.Dag;

namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Décrit les canaux d'entrée et sortie à injecter dans une branche
/// lors de son exécution par l'orchestrateur.
/// Remplace l'injection d'arguments CLI dans DagOrchestrator.
/// </summary>
public record BranchChannelContext
{
    /// <summary>Alias du canal mémoire à lire (null = lire depuis job.Input directement).</summary>
    public string? InputChannelAlias { get; init; }
    
    /// <summary>Type de canal d'entrée (Arrow ou Native). Null si lecture externe.</summary>
    public ChannelMode? InputChannelMode { get; init; }
    
    /// <summary>Alias du canal de sortie mémoire (null = écrire vers job.Output directement).</summary>
    public string? OutputChannelAlias { get; init; }
    
    /// <summary>Type de canal de sortie.</summary>
    public ChannelMode OutputChannelMode { get; init; } = ChannelMode.Native;
    
    /// <summary>Mapping logique→physique pour les sous-canaux de fan-out.</summary>
    public IReadOnlyDictionary<string, string> AliasMap { get; init; } 
        = new Dictionary<string, string>();
}
