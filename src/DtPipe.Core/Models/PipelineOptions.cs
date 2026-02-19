namespace DtPipe.Core.Models;

/// <summary>
/// Pipeline execution configuration — CLI-neutral.
/// Can be constructed directly by any application, without depending on DtPipe CLI types.
/// </summary>
public sealed record PipelineOptions
{
	// --- Execution ---

	/// <summary>Number of rows per read/write batch. Default: 50 000.</summary>
	public int BatchSize { get; init; } = 50_000;

	/// <summary>Maximum rows to process. 0 = unlimited.</summary>
	public int Limit { get; init; } = 0;

	/// <summary>Maximum number of retries on transient write errors. Default: 3.</summary>
	public int MaxRetries { get; init; } = 3;

	/// <summary>Initial delay in ms between retries (doubles each attempt). Default: 1000.</summary>
	public int RetryDelayMs { get; init; } = 1000;

	// --- Sampling ---

	/// <summary>Probability 0.0–1.0 to include a row. 1.0 = all rows (default).</summary>
	public double SamplingRate { get; init; } = 1.0;

	/// <summary>Seed for reproducible sampling. Null = random.</summary>
	public int? SamplingSeed { get; init; }

	// --- Schema Validation ---

	/// <summary>Abort if schema incompatibilities are detected (strict mode).</summary>
	public bool StrictSchema { get; init; } = false;

	/// <summary>Disable pre-write schema compatibility validation.</summary>
	public bool NoSchemaValidation { get; init; } = false;

	/// <summary>Automatically add missing columns to target table.</summary>
	public bool AutoMigrate { get; init; } = false;

	// --- Dry Run ---

	/// <summary>Number of rows to preview without writing. 0 = disabled.</summary>
	public int DryRunCount { get; init; } = 0;

	// --- Lifecycle Hooks ---

	/// <summary>SQL / command to execute before data transfer.</summary>
	public string? PreExec { get; init; }

	/// <summary>SQL / command to execute after successful data transfer.</summary>
	public string? PostExec { get; init; }

	/// <summary>SQL / command to execute if an error occurs.</summary>
	public string? OnErrorExec { get; init; }

	/// <summary>SQL / command to execute always (finally).</summary>
	public string? FinallyExec { get; init; }

	// --- Observability ---

	/// <summary>Disable live progress statistics (useful for CI).</summary>
	public bool NoStats { get; init; } = false;

	/// <summary>Path to save structured metrics as JSON. Null = skip.</summary>
	public string? MetricsPath { get; init; }
}
