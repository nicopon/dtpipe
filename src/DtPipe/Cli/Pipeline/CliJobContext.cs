namespace DtPipe.Cli.Pipeline;

/// <summary>
/// Carries CLI-origin arg slices for one pipeline branch.
/// Produced by PipelineToJobConverter alongside JobDefinition.
/// Kept separate so JobDefinition (DtPipe.Core) remains CLI-agnostic.
/// </summary>
public record CliJobContext(
    string[]? ReaderArguments,
    string[]? PipelineArguments,
    string[]? WriterArguments,
    string[]? Arguments
);
