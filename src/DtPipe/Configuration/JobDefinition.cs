namespace DtPipe.Configuration;
/// <summary>
/// Central job definition for export configuration, hydrated from CLI or YAML.
/// </summary>
public record JobDefinition
{
	public required string Input { get; init; }
	public string? Query { get; init; }
	public required string Output { get; init; }
	public int BatchSize { get; init; } = 50_000;

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

	public int ConnectionTimeout { get; init; } = 10;
	public int QueryTimeout { get; init; } = 0;

	public int MaxRetries { get; init; } = 3;
	public int RetryDelayMs { get; init; } = 1000;
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

	/// <summary>Provider-specific options. Keyed by provider name (e.g. 'oracle-writer').</summary>
	public Dictionary<string, Dictionary<string, object>>? ProviderOptions { get; init; }
}

public record TransformerConfig
{
	public required string Type { get; init; }

	/// <summary>
	/// Mapping property aliased to specific types for YAML deserialization convenience.
	/// </summary>
	public Dictionary<string, string>? Mappings { get; init; }

	public Dictionary<string, string>? Mask => Type == "mask" ? Mappings : null;
	public Dictionary<string, string>? Fake => Type == "fake" ? Mappings : null;
	public Dictionary<string, string>? Format => Type == "format" ? Mappings : null;
	public Dictionary<string, string>? Script => Type == "script" ? Mappings : null;
	public Dictionary<string, string>? Overwrite => Type == "overwrite" ? Mappings : null;

	public Dictionary<string, string>? Options { get; init; }
}
