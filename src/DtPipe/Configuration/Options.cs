using DtPipe.Core.Options;

namespace DtPipe.Configuration;

/// <summary>
/// Export configuration options.
/// </summary>
public sealed record DumpOptions : IOptionSet
{
	// --- General Options ---
	public string Provider { get; init; } = "oracle";
	public string ConnectionString { get; init; } = "";
	public string? Query { get; init; } = "";
	public string OutputPath { get; init; } = "";

	public static string Prefix => "global";
	public static string DisplayName => "Global Options";

	// --- Generic Write Options ---
	public string? Strategy { get; init; }
	public string? InsertMode { get; init; }
	public string? Table { get; init; }

	// --- Execution Options ---
	public int ConnectionTimeout { get; init; } = 10; // seconds
	public int QueryTimeout { get; init; } = 0; // 0 = no timeout

	/// <summary>
	/// Maximum number of retries for transient errors (0 = no retry). Default: 3.
	/// </summary>
	public int MaxRetries { get; init; } = 3;

	/// <summary>
	/// Initial delay in milliseconds between retries (doubles at each attempt). Default: 1000.
	/// </summary>
	public int RetryDelayMs { get; init; } = 1000;

	// --- Output Options ---
	/// <summary>
	/// Size of the batch for reading from source and writing to output.
	/// Controls memory conversion buffer size and Parquet RowGroup size.
	/// </summary>
	public int BatchSize { get; init; } = 50_000;

	// --- Safety Options ---
	/// <summary>
	/// If true, bypasses SQL query validation (allows DDL/DML operations).
	/// Use with extreme caution!
	/// </summary>
	public bool UnsafeQuery { get; init; } = false;

	/// <summary>
	/// If true, disables the live progress statistics (useful for CI/logs).
	/// </summary>
	public bool NoStats { get; init; } = false;

	/// <summary>
	/// If true, aborts the export if schema incompatibilities (errors) are detected.
	/// </summary>
	public bool StrictSchema { get; init; } = false;

	/// <summary>
	/// If true, disables pre-write schema compatibility validation.
	/// </summary>
	public bool NoSchemaValidation { get; init; } = false;

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
	public double SamplingRate { get; init; } = 1.0;

	/// <summary>
	/// Seed for random sampling to ensure reproducibility.
	/// If null, a system-generated seed is used.
	/// </summary>
	public int? SamplingSeed { get; init; }

	/// <summary>
	/// Path to the log file. If set, file logging is enabled.
	/// </summary>
	public string? LogPath { get; init; }
	/// <summary>
	/// Comma-separated list of primary key columns for Upsert/Ignore strategies.
	/// If null, auto-detection via schema inspection is attempted.
	/// </summary>
	public string? Key { get; init; }
	/// <summary>
	/// Path to save structured metrics (JSON). If null, metrics are only logged/displayed.
	/// </summary>
	public string? MetricsPath { get; init; }

	/// <summary>Automatically add missing columns to target table.</summary>
	public bool AutoMigrate { get; init; } = false;

	// --- Lifecycle Hooks ---
	/// <summary>
	/// Command to execute before data transfer (Pre-Execution).
	/// </summary>
	public string? PreExec { get; init; }

	/// <summary>
	/// Command to execute after successful data transfer (Post-Execution).
	/// </summary>
	public string? PostExec { get; init; }

	/// <summary>
	/// Command to execute if an error occurs (On-Error).
	/// </summary>
	public string? OnErrorExec { get; init; }

	/// <summary>
	/// Command to execute always after transfer (Finally).
	/// </summary>
	public string? FinallyExec { get; init; }
}
