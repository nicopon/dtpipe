using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Core.Models;
/// <summary>
/// Central job definition for export configuration, hydrated from CLI or YAML.
/// </summary>
public record JobDefinition
{
	public string? Input { get; init; }
	public string? Query { get; init; }
	public string? Output { get; init; }
	public int BatchSize { get; init; } = 50_000;

    /// <summary>
    /// The transformation logic to apply if using a processor engine (e.g. DataFusion SQL).
    /// </summary>
    public string? Sql { get; init; }

	/// <summary>Append, Truncate, Recreate, etc.</summary>
	public string? Strategy { get; init; }

	/// <summary>Standard, Bulk, Binary.</summary>
	public string? InsertMode { get; init; }

	public string? Table { get; init; }
	public int Limit { get; init; } = 0;
	public bool DryRun { get; init; } = false;
	public bool UnsafeQuery { get; init; } = false;
	public bool StrictSchema { get; init; } = false;
	public bool NoSchemaValidation { get; init; } = false;
	public bool NoStats { get; init; } = false;

	public int ConnectionTimeout { get; init; } = 10;
	public int QueryTimeout { get; init; } = 0;

	public string? MetricsPath { get; init; }
    public bool? AutoMigrate { get; set; }
    public int Throttle { get; init; } = 0;
    public bool IgnoreNulls { get; init; } = false;

	public List<TransformerConfig>? Transformers { get; init; }
	public string? LogPath { get; init; }
	public string? Key { get; init; }

	public double SamplingRate { get; init; } = 1.0;
	public int? SamplingSeed { get; init; }

    public string[] Drop { get; init; } = Array.Empty<string>();
    public string[] Rename { get; init; } = Array.Empty<string>();

	// Lifecycle Hooks
	public string? PreExec { get; init; }
	public string? PostExec { get; init; }
	public string? OnErrorExec { get; init; }
	public string? FinallyExec { get; init; }

    public string? Prefix { get; init; }

    // Routing/DAG Properties
    public string[] Ref { get; init; } = Array.Empty<string>();
    public string? From { get; set; }

	/// <summary>Provider-specific options. Keyed by provider name (e.g. 'oracle-writer').</summary>
	public Dictionary<string, Dictionary<string, object>>? ProviderOptions { get; init; }
}



