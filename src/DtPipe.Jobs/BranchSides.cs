namespace DtPipe.Jobs;

/// <summary>
/// Which keys of a branch concern its reader and which its writer, as a job file names them. What
/// <c>dtpipe split</c> moves to each half of a cut, and what the designer gives a source card and a
/// sink card: one list, so the two never place a key differently.
/// </summary>
public static class BranchSides
{
    /// <summary>Keys that bound, resume or sample what the reader reads.</summary>
    public static readonly IReadOnlySet<string> Read = new HashSet<string>(
        ["cursor", "state", "limit", "sampling-rate", "sampling-seed", "from-checkpoint", "from-contract"], StringComparer.Ordinal);

    /// <summary>Keys that govern what the writer writes.</summary>
    public static readonly IReadOnlySet<string> Write = new HashSet<string>(
        ["prefix", "checkpoint", "contract-save"], StringComparer.Ordinal);

    /// <summary>
    /// Keys that describe one whole run: a path cannot describe two, so no side takes them.
    /// </summary>
    public static readonly IReadOnlySet<string> Diagnostics = new HashSet<string>(
        ["metrics-path", "log-path"], StringComparer.Ordinal);
}
