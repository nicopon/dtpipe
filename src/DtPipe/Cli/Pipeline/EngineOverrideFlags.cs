namespace DtPipe.Cli.Pipeline;

/// <summary>
/// F11 — the flags a command line may give beside <c>--job</c>: the engine controls
/// <c>PipelineToJobConverter</c> applies to every branch of the loaded job. The lexer refuses any other
/// flag of a branch there, since a job file is the source of the branches and a flag the conversion
/// does not read would be dropped in silence. A flag is listed only when the conversion reads it.
/// <c>--metrics-path</c>, <c>--dry-run</c> and <c>--session</c> are flags of the whole command line,
/// which no branch ever holds; the others are flags of a branch.
/// </summary>
public static class EngineOverrideFlags
{
    public static readonly IReadOnlyList<string> All =
    [
        "--limit", "--batch-size", "-b", "--max-batch-bytes", "--metrics-path",
        "--cursor", "--state", "--sampling-rate", "--sampling-seed",
        "--prefix", "--dry-run", "--session",
    ];
}
