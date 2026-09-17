using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Core.Models;

/// <summary>
/// Central job definition for export configuration, hydrated from CLI or YAML.
/// Adapter-specific fields (Query, Table, Strategy, Key, hooks, schema persistence)
/// are handled directly by OptionBinder.BindCli → adapter option POCOs (CLI path)
/// or OptionBinder.BindYaml ← ProviderOptions (YAML path).
/// </summary>
public record JobDefinition
{
	public string? Input { get; init; }
	public string? Output { get; init; }
	public int BatchSize { get; init; } = PipelineOptions.DefaultBatchSize;
	public long MaxBatchBytes { get; init; } = 0;
	public int Limit { get; init; } = 0;
	public bool NoStats { get; init; } = false;
	public int DryRunCount { get; init; } = 0;

	/// <summary>Materialise this branch's output in the session store (--checkpoint).</summary>
	public string? Checkpoint { get; init; }

	/// <summary>Resume this branch from a stored checkpoint instead of its input (--from-checkpoint).</summary>
	public string? FromCheckpoint { get; init; }

	/// <summary>Where to write the schema this branch produces (--contract-save).</summary>
	public string? ContractSave { get; init; }

	/// <summary>Session the checkpoints belong to (--session); null lets the precedence chain decide.</summary>
	public string? Session { get; init; }
	public string? MetricsPath { get; init; }
	public string? LogPath { get; init; }

	public double SamplingRate { get; init; } = 1.0;
	public int? SamplingSeed { get; init; }

    public string? Prefix { get; init; }

	// Incremental loading
	public string? Cursor { get; init; }
	public string? State { get; init; }

	public List<TransformerConfig>? Transformers { get; init; }

    // Routing/DAG Properties
    public string[] Ref { get; init; } = Array.Empty<string>();
    public string? From { get; set; }

	/// <summary>Provider-specific options. Keyed by provider name (e.g. 'oracle-writer').</summary>
	public Dictionary<string, Dictionary<string, object?>>? ProviderOptions { get; init; }
}

