using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Core.Models;

/// <summary>
/// Universal pipeline execution controls — independent of any specific adapter.
/// Adapter-specific flags (schema validation, hooks, query, key, table, strategy, schema persistence)
/// live in their respective options classes and are accessed via ISchemaValidationAware, IHookAware,
/// ISchemaPersistenceAware, IQueryAwareOptions, IKeyAwareOptions.
/// </summary>
public sealed record PipelineOptions : IOptionSet
{
	public static string Prefix => "global";
	public static string DisplayName => "Global Options";

	public const int DefaultBatchSize = 32_768;

	[ComponentOption("--batch-size", Aliases = new[] { "-b" }, Description = "Batch size for processing")]
	public int BatchSize { get; init; } = DefaultBatchSize;

	[ComponentOption("--max-batch-bytes", Description = "Soft byte cap per read batch; flush when either --batch-size rows or this many bytes accumulate (0 = no byte cap)")]
	public long MaxBatchBytes { get; init; } = 0;

	[ComponentOption("--limit", Description = "Max total rows to process (0 = unlimited)")]
	public int Limit { get; init; } = 0;

	[ComponentOption("--sampling-rate", Aliases = new[] { "--sample-rate" }, Description = "Sampling rate (0.0–1.0, 1.0 = all rows)")]
	public double SamplingRate { get; init; } = 1.0;

	[ComponentOption("--sampling-seed", Aliases = new[] { "--sample-seed" }, Description = "Seed for deterministic sampling")]
	public int? SamplingSeed { get; init; }

	[ComponentOption("--ignore-nulls", Description = "Skip null values in processing")]
	public bool IgnoreNulls { get; init; }

	[ComponentOption("--retry", Description = "Enable exponential backoff retry policy for transient database/network errors (3 attempts)")]
	public bool Retry { get; init; } = false;

	// --- Execution controls (not CLI flags; set by LinearPipelineService from JobDefinition) ---
	public string? MetricsPath { get; init; }
	public bool NoStats { get; init; } = false;
	public int DryRunCount { get; init; } = 0;
	public string? DryRunInteractiveBranch { get; init; }

	// Incremental loading — set from JobDefinition by LinearPipelineService
	public string? Cursor { get; init; }
	public string? State { get; init; }

	// Materialisation — set from JobDefinition by LinearPipelineService
	public string? Checkpoint { get; init; }
	public string? FromCheckpoint { get; init; }

	// Contract — set from JobDefinition by LinearPipelineService
	public string? ContractSave { get; init; }
	public string? FromContract { get; init; }
	public string? Session { get; init; }
}
