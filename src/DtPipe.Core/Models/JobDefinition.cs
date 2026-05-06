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
    /// The transformation logic to apply if using a processor engine (e.g. DuckDB SQL).
    /// </summary>
    public string? Sql { get; init; }

	/// <summary>Append, Truncate, Recreate, etc.</summary>
	public string? Strategy { get; init; }

	/// <summary>Standard, Bulk, Binary.</summary>
	public string? InsertMode { get; init; }

	public string? Table { get; init; }
	public int Limit { get; init; } = 0;
	public bool StrictSchema { get; init; } = false;
	public bool NoSchemaValidation { get; init; } = false;
	public bool NoStats { get; init; } = false;
	public int DryRunCount { get; init; } = 0;

	public string? MetricsPath { get; init; }
    public bool? AutoMigrate { get; set; }

	public List<TransformerConfig>? Transformers { get; init; }
	public string? LogPath { get; init; }
	public string? Key { get; init; }

	public double SamplingRate { get; init; } = 1.0;
	public int? SamplingSeed { get; init; }

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

	/// <summary>Save discovered schema to a named .dtschema file after OpenAsync.</summary>
	public string? SchemaSave { get; init; }

	/// <summary>Load ColumnTypes from a named .dtschema file, bypassing schema inference.</summary>
	public string? SchemaLoad { get; init; }

	/// <summary>
	/// Full Arrow schema as compact JSON. Set by --export-job; consumed by --job to skip inference.
	/// Not a CLI flag — managed exclusively via --schema-save / --schema-load / --export-job.
	/// </summary>
	public string? Schema { get; init; }

	/// <summary>Raw CLI arguments for this branch (full flat list — union of the three below).</summary>
	public string[]? Arguments { get; set; }

	/// <summary>Flags in the reader scope: from start to first transformer trigger or -o.</summary>
	public string[]? ReaderArguments { get; set; }

	/// <summary>Flags in the transformer scope: from first transformer trigger to -o.</summary>
	public string[]? PipelineArguments { get; set; }

	/// <summary>Flags in the writer scope: from -o to end.</summary>
	public string[]? WriterArguments { get; set; }
}
