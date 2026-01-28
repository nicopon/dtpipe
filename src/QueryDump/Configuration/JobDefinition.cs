namespace QueryDump.Configuration;

/// <summary>
/// Central job definition that can be hydrated from CLI args or YAML file.
/// All export configuration is contained here.
/// </summary>
public record JobDefinition
{
    /// <summary>
    /// Input connection string or file path.
    /// </summary>
    public required string Input { get; init; }

    /// <summary>
    /// SQL query to execute.
    /// </summary>
    public string? Query { get; init; }

    /// <summary>
    /// Output file path.
    /// </summary>
    public required string Output { get; init; }

    /// <summary>
    /// Rows per output batch.
    /// </summary>
    public int BatchSize { get; init; } = 50_000;

    /// <summary>
    /// Maximum rows to export (0 = unlimited).
    /// </summary>
    public int Limit { get; init; } = 0;

    /// <summary>
    /// Display schema without exporting data.
    /// </summary>
    public bool DryRun { get; init; } = false;

    /// <summary>
    /// Bypass SQL query validation.
    /// </summary>
    public bool UnsafeQuery { get; init; } = false;

    /// <summary>
    /// Connection timeout in seconds.
    /// </summary>
    public int ConnectionTimeout { get; init; } = 10;

    /// <summary>
    /// Query timeout in seconds (0 = no timeout).
    /// </summary>
    public int QueryTimeout { get; init; } = 0;

    /// <summary>
    /// Transformer configurations from YAML.
    /// </summary>
    public List<TransformerConfig>? Transformers { get; init; }

    /// <summary>
    /// Path to the log file.
    /// </summary>
    public string? LogPath { get; init; }

    /// <summary>
    /// Probability 0.0-1.0 to include a row.
    /// </summary>
    public double SampleRate { get; init; } = 1.0;

    /// <summary>
    /// Seed for random sampling.
    /// </summary>
    /// <summary>
    /// Seed for random sampling.
    /// </summary>
    public int? SampleSeed { get; init; }

    /// <summary>
    /// Provider-specific options (e.g. oracle-writer: { table: "MY_TABLE" }).
    /// </summary>
    public Dictionary<string, Dictionary<string, object>>? ProviderOptions { get; init; }
}

/// <summary>
/// Configuration for a single transformer in YAML.
/// </summary>
public record TransformerConfig
{
    /// <summary>
    /// Transformer type (fake, format, null, overwrite).
    /// </summary>
    public required string Type { get; init; }

    /// <summary>
    /// Mappings for this transformer (column -> value).
    /// Aliased to 'mask', 'fake', 'format', 'script', 'overwrite' depending on type.
    /// </summary>
    public Dictionary<string, string>? Mappings { get; init; }
    
    public Dictionary<string, string>? Mask => Type == "mask" ? Mappings : null;
    public Dictionary<string, string>? Fake => Type == "fake" ? Mappings : null;
    public Dictionary<string, string>? Format => Type == "format" ? Mappings : null;
    public Dictionary<string, string>? Script => Type == "script" ? Mappings : null;
    public Dictionary<string, string>? Overwrite => Type == "overwrite" ? Mappings : null;

    /// <summary>
    /// Additional options (e.g., locale, seed for fake).
    /// </summary>
    public Dictionary<string, string>? Options { get; init; }
}
