namespace DtPipe.Cli.Pipeline;

/// <summary>
/// F11 — single source of truth for the engine-control CLI flags that may override a
/// loaded YAML job's values. Both the job-file override pass and engine-settings
/// derivation consume this list.
/// </summary>
public static class EngineOverrideFlags
{
    public static readonly IReadOnlyList<string> All =
    [
        "--limit", "--batch-size", "-b", "--max-batch-bytes", "--log", "--metrics-path",
        "--cursor", "--state", "--sampling-rate", "--sampling-seed",
        "--prefix", "--dry-run", "--session", "--checkpoint", "--from-checkpoint", "--contract-save",
    ];
}
