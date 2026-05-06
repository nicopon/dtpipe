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

	[ComponentOption("--batch-size", Aliases = new[] { "-b" }, Description = "Batch size for processing")]
	public int BatchSize { get; init; } = 50_000;

	[ComponentOption("--limit", Description = "Max total rows to process (0 = unlimited)")]
	public int Limit { get; init; } = 0;

	[ComponentOption("--sampling-rate", Aliases = new[] { "--sample-rate" }, Description = "Sampling rate (0.0–1.0, 1.0 = all rows)")]
	public double SamplingRate { get; init; } = 1.0;

	[ComponentOption("--sampling-seed", Aliases = new[] { "--sample-seed" }, Description = "Seed for deterministic sampling")]
	public int? SamplingSeed { get; init; }

	// --- Execution controls (not CLI flags; set by LinearPipelineService from JobDefinition) ---
	public string? MetricsPath { get; init; }
	public bool NoStats { get; init; } = false;
	public int DryRunCount { get; init; } = 0;
}
