using DtPipe.Core.Options;

namespace DtPipe.Core.Models;

/// <summary>
/// F7 — canonical per-branch engine-control settings (limit/batch/sampling/dry-run/
/// metrics/log/prefix/cursor/state). The CLI converter derives the bundle once, from global
/// defaults overlaid by branch-local flags, and immediately folds it into the branch's job
/// through <see cref="ApplyTo"/>. Downstream the settings are read off the
/// <see cref="JobDefinition"/>; the bundle itself is the derivation step, not a second place
/// to look them up.
/// </summary>
public sealed record BranchEngineSettings(
    int Limit,
    int BatchSize,
    long MaxBatchBytes,
    double SamplingRate,
    int? SamplingSeed,
    int DryRunCount,
    bool NoStats,
    string? MetricsPath,
    string? LogPath,
    string? Prefix,
    string? Cursor,
    string? State,
    // Checkpoint: materialise this branch's output in the session store (--checkpoint).
    // FromCheckpoint: read its source from the store instead of the input (--from-checkpoint).
    string? Checkpoint = null,
    string? FromCheckpoint = null)
{
    public static BranchEngineSettings Default { get; }
        = new(Limit: 0, BatchSize: PipelineOptions.DefaultBatchSize, MaxBatchBytes: 0, SamplingRate: 1.0, SamplingSeed: null,
              DryRunCount: 0, NoStats: false, MetricsPath: null, LogPath: null, Prefix: null,
              Cursor: null, State: null);

    /// <summary>Applies these settings onto a job definition (single derivation point).</summary>
    public JobDefinition ApplyTo(JobDefinition job) => job with
    {
        Limit = Limit,
        BatchSize = BatchSize,
        MaxBatchBytes = MaxBatchBytes,
        SamplingRate = SamplingRate,
        SamplingSeed = SamplingSeed,
        DryRunCount = DryRunCount,
        NoStats = NoStats || job.NoStats,
        MetricsPath = MetricsPath ?? job.MetricsPath,
        LogPath = LogPath ?? job.LogPath,
        Prefix = Prefix ?? job.Prefix,
        Cursor = Cursor ?? job.Cursor,
        State = State ?? job.State,
        Checkpoint = Checkpoint ?? job.Checkpoint,
        FromCheckpoint = FromCheckpoint ?? job.FromCheckpoint,
    };
}

/// <summary>
/// F7 — canonical description of one pipeline branch: DAG routing plus its engine
/// settings. This is the authoritative model; JobDefinition carries provider-level data
/// and CliJobContext only transient binding info.
/// </summary>
public sealed record Branch(
    string Alias,
    string? Input,
    string? Output,
    IReadOnlyList<string> StreamingAliases,
    IReadOnlyList<string> RefAliases,
    string? ProcessorName)
{
    public bool HasStreamTransformer => ProcessorName != null;
}
