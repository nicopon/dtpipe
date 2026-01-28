namespace QueryDump.Configuration;

/// <summary>
/// Export configuration options.
/// </summary>
public sealed record DumpOptions
{
    // --- General Options ---
    public string Provider { get; init; } = "oracle";
    public required string ConnectionString { get; init; }
    public required string? Query { get; init; }
    public required string OutputPath { get; init; }
    
    // --- Execution Options ---
    public int ConnectionTimeout { get; init; } = 10; // seconds
    public int QueryTimeout { get; init; } = 0; // 0 = no timeout
    
    // --- Output Options ---
    /// <summary>
    /// Size of the batch for reading from source and writing to output.
    /// Controls memory conversion buffer size and Parquet RowGroup size.
    /// </summary>
    public int BatchSize { get; init; } = 5_000; 

    // --- Safety Options ---
    /// <summary>
    /// If true, bypasses SQL query validation (allows DDL/DML operations).
    /// Use with extreme caution!
    /// </summary>
    public bool UnsafeQuery { get; init; } = false;

    // --- Validation Options ---
    /// <summary>
    /// Number of sample rows to collect for dry-run trace analysis.
    /// 0 = disabled, 1+ = sample count.
    /// </summary>
    public int DryRunCount { get; init; } = 0;

    /// <summary>
    /// Maximum number of rows to export. 0 = unlimited.
    /// </summary>
    public int Limit { get; init; } = 0;

    /// <summary>
    /// Probability 0.0-1.0 to include a row.
    /// 1.0 = Include all (default). 0.1 = Include 10%.
    /// </summary>
    public double SampleRate { get; init; } = 1.0;

    /// <summary>
    /// Seed for random sampling to ensure reproducibility.
    /// If null, a system-generated seed is used.
    /// </summary>
    public int? SampleSeed { get; init; }

    /// <summary>
    /// Path to the log file. If set, file logging is enabled.
    /// </summary>
    public string? LogPath { get; init; }
}
