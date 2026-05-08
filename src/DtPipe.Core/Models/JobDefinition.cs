using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Core.Models;

/// <summary>
/// Central job definition for export configuration, hydrated from CLI or YAML.
/// Adapter-specific fields (Query, Table, Strategy, Key, hooks, schema persistence)
/// are handled directly by FlagBinder → adapter option POCOs (CLI path)
/// or ConfigurationBinder → ProviderOptions (YAML path).
/// </summary>
public record JobDefinition
{
	public string? Input { get; init; }
	public string? Output { get; init; }
	public int BatchSize { get; init; } = 50_000;
	public int Limit { get; init; } = 0;
	public bool NoStats { get; init; } = false;
	public int DryRunCount { get; init; } = 0;
	public string? MetricsPath { get; init; }
	public string? LogPath { get; init; }

	public double SamplingRate { get; init; } = 1.0;
	public int? SamplingSeed { get; init; }

    public string? Prefix { get; init; }

	public List<TransformerConfig>? Transformers { get; init; }

    // Routing/DAG Properties
    public string[] Ref { get; init; } = Array.Empty<string>();
    public string? From { get; set; }

	/// <summary>Provider-specific options. Keyed by provider name (e.g. 'oracle-writer').</summary>
	public Dictionary<string, Dictionary<string, object>>? ProviderOptions { get; init; }
}

