namespace DtPipe.Cli.Pipeline;

/// <summary>The implicit branch-split decision produced by <see cref="BranchSplitDecision"/>.</summary>
public enum SplitDecision
{
    /// <summary>Token belongs to the current branch.</summary>
    Stay,
    /// <summary>A second -i/--input: flush the current branch and start a new one.</summary>
    NewInput,
    /// <summary>A --from after existing input/job/from: start a new consumer branch.</summary>
    NewFrom,
    /// <summary>A --ref after an input or job and no --from: start a consumer branch with no streamed source.</summary>
    NewRef,
    /// <summary>A second --job/-j: flush and start a new job branch.</summary>
    NewJob,
}

/// <summary>State accumulated by the lexer while walking a branch.</summary>
/// <param name="HasInput">An -i/--input was already seen in the current branch.</param>
/// <param name="HasJob">A --job/-j was already seen in the current branch.</param>
/// <param name="HasFrom">A --from was already seen in the current branch.</param>
public readonly record struct BranchSplitState(bool HasInput, bool HasJob, bool HasFrom);

/// <summary>
/// F6 — single pure function deciding implicit DAG branch splits. The lexer's main loop
/// and shell completion both consume it, so parsing semantics have exactly one definition.
///
/// Triggers (documented in REFERENCE.md §DAG Syntax):
/// - second -i/--input when an input or job file was already seen;
/// - any --from when a --from, --input or --job was already seen (first --from in a fresh
///   branch stays in the current branch);
/// - second --job/-j when a job file or input was already seen;
/// - --ref when an input or job file was already seen and no --from was: the branch that follows
///   has no streamed source, only materialised ones. A branch that already has a --from keeps its
///   --ref, and one opened by --ref has no input, so a second --ref stays in it and is refused.
/// Neither --sql nor boolean processor flags trigger a split.
/// </summary>
public static class BranchSplitDecision
{
    public static SplitDecision Decide(BranchSplitState s, string token)
    {
        if (token.Equals("--from", StringComparison.OrdinalIgnoreCase))
        {
            return (s.HasFrom || s.HasInput || s.HasJob) ? SplitDecision.NewFrom : SplitDecision.Stay;
        }

        if (token.Equals("-i", StringComparison.OrdinalIgnoreCase) || token.Equals("--input", StringComparison.OrdinalIgnoreCase))
        {
            return (s.HasInput || s.HasJob) ? SplitDecision.NewInput : SplitDecision.Stay;
        }

        if (token.Equals("--ref", StringComparison.OrdinalIgnoreCase))
        {
            return (s.HasInput || s.HasJob) && !s.HasFrom ? SplitDecision.NewRef : SplitDecision.Stay;
        }

        if (token.Equals("--job", StringComparison.OrdinalIgnoreCase) || token.Equals("-j", StringComparison.OrdinalIgnoreCase))
        {
            return (s.HasJob || s.HasInput) ? SplitDecision.NewJob : SplitDecision.Stay;
        }

        return SplitDecision.Stay;
    }
}
