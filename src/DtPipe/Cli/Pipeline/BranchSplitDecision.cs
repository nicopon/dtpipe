namespace DtPipe.Cli.Pipeline;

/// <summary>The implicit branch-split decision produced by <see cref="BranchSplitDecision"/>.</summary>
public enum SplitDecision
{
    /// <summary>Token belongs to the current branch.</summary>
    Stay,
    /// <summary>A second -i/--input: flush the current branch and start a new one.</summary>
    NewInput,
    /// <summary>A --from after an existing input or --from: start a new consumer branch.</summary>
    NewFrom,
}

/// <summary>State accumulated by the lexer while walking a branch.</summary>
/// <param name="HasInput">An -i/--input was already seen in the current branch.</param>
/// <param name="HasFrom">A --from was already seen in the current branch.</param>
public readonly record struct BranchSplitState(bool HasInput, bool HasFrom);

/// <summary>
/// F6 — single pure function deciding implicit DAG branch splits. The lexer's main loop consumes
/// it, so parsing semantics have exactly one definition.
///
/// Triggers (documented in REFERENCE.md §DAG Syntax):
/// - second -i/--input when an input was already seen;
/// - any --from when a --from or --input was already seen (first --from in a fresh branch stays
///   in the current branch).
/// Neither --sql nor boolean processor flags trigger a split, and neither does --job: it is a
/// global flag, given once per command line.
/// </summary>
public static class BranchSplitDecision
{
    public static SplitDecision Decide(BranchSplitState s, string token)
    {
        if (token.Equals("--from", StringComparison.OrdinalIgnoreCase))
        {
            return (s.HasFrom || s.HasInput) ? SplitDecision.NewFrom : SplitDecision.Stay;
        }

        if (token.Equals("-i", StringComparison.OrdinalIgnoreCase) || token.Equals("--input", StringComparison.OrdinalIgnoreCase))
        {
            return s.HasInput ? SplitDecision.NewInput : SplitDecision.Stay;
        }

        return SplitDecision.Stay;
    }
}
