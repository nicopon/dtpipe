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
    public required string Query { get; init; }

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
    /// </summary>
    public Dictionary<string, string>? Mappings { get; init; }

    /// <summary>
    /// Additional options (e.g., locale, seed for fake).
    /// </summary>
    public Dictionary<string, string>? Options { get; init; }
}
