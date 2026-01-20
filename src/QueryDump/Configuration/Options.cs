namespace QueryDump.Configuration;

/// <summary>
/// Export configuration options.
/// </summary>
public sealed record DumpOptions
{
    // --- General Options ---
    public string Provider { get; init; } = "oracle";
    public required string ConnectionString { get; init; }
    public required string Query { get; init; }
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
    /// If true, only displays the query schema without exporting data.
    /// </summary>
    public bool DryRun { get; init; } = false;

    /// <summary>
    /// Maximum number of rows to export. 0 = unlimited.
    /// </summary>
    public int Limit { get; init; } = 0;
}
