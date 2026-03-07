namespace DtPipe.Cli;

/// <summary>
/// Result of the partial command line analysis up to the cursor.
/// Produced by CliContextAnalyzer, consumed by the suggestion engine.
/// </summary>
public sealed record CliCompletionContext
{
    /// <summary>Index of the current branch (0-based).</summary>
    public int CurrentBranchIndex { get; init; }

    /// <summary>Was the current branch initiated by -x/--xstreamer?</summary>
    public bool IsXStreamerBranch { get; init; }

    /// <summary>Last complete flag typed that was expecting a value. Ex: "--main", "--ref".</summary>
    public string? LastCompletedFlag { get; init; }

    /// <summary>Flags already used in the current branch (long names, e.g. "--input").</summary>
    public IReadOnlySet<string> UsedFlagsInCurrentBranch { get; init; } = new HashSet<string>();

    /// <summary>--alias aliases declared in FINALIZED branches (not the current one).</summary>
    public IReadOnlyList<string> KnownAliases { get; init; } = Array.Empty<string>();

    /// <summary>Prefix of the --input in the current branch. Ex: "pg:" if --input pg:orders. Null if absent.</summary>
    public string? CurrentInputPrefix { get; init; }

    /// <summary>True if the last token is a flag still expecting its value.</summary>
    public bool IsExpectingFlagValue { get; init; }

    /// <summary>True if --output or --alias has already been provided in the current branch.</summary>
    public bool HasOutput { get; init; }

    /// <summary>All available source flags (long names) for re-injection in Phase 2.</summary>
    public IReadOnlyList<string> AllSourceFlags { get; init; } = Array.Empty<string>();

    /// <summary>
    /// Active pipeline phase at the cursor position.
    /// Derived from HasOutput, CurrentInputPrefix, and IsXStreamerBranch.
    /// </summary>
    public CliPipelinePhase ActivePhase { get; init; }
}
