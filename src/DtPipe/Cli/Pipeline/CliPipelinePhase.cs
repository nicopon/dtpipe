using System;

namespace DtPipe.Cli;

/// <summary>
/// Indicates during which phase of the pipeline construction a CLI option is relevant.
/// Used by autocompletion to suppress irrelevant suggestions.
/// </summary>
[Flags]
public enum CliPipelinePhase
{
    /// <summary>Always visible regardless of context.</summary>
    Global = 0,

    /// <summary>Visible only when the active reader prefix matches this component.</summary>
    Reader = 1,

    /// <summary>Visible once a source is defined (Phase 1: input defined, no output yet).</summary>
    Transformer = 2,

    /// <summary>Visible only after --output has been specified in the current branch.</summary>
    Writer = 4,

    /// <summary>Visible only in a Processor branch (initiated by --sql).</summary>
    Processor = 8,
}
