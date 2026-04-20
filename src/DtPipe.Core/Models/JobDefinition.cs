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

	/// <summary>Save discovered schema to a named .dtschema file after OpenAsync.</summary>
	public string? SchemaSave { get; init; }

	/// <summary>Load ColumnTypes from a named .dtschema file, bypassing schema inference.</summary>
	public string? SchemaLoad { get; init; }

	// --- Universal Reader Options (per-branch, apply to the branch's reader) ---

	/// <summary>Navigation path in the source: dot-path for JSON (e.g. "items.data"), XPath for XML (e.g. "//Record").</summary>
	public string? Path { get; init; }

	/// <summary>Explicit column types, e.g. "Id:uuid,Count:int64,Active:bool".</summary>
	public string? ColumnTypes { get; init; }

	/// <summary>Automatically infer and apply column types from the first sample rows.</summary>
	public bool AutoColumnTypes { get; init; } = false;

	/// <summary>Maximum rows to sample for schema inference (0 = reader default).</summary>
	public int MaxSample { get; init; } = 0;

	/// <summary>File encoding (e.g., UTF-8, ISO-8859-1). Defaults to UTF-8.</summary>
	public string? Encoding { get; init; }

	/// <summary>
	/// Full Arrow schema as compact JSON. Set by --export-job; consumed by --job to skip inference.
	/// Not a CLI flag — managed exclusively via --schema-save / --schema-load / --export-job.
	/// </summary>
	public string? Schema { get; init; }
}



