using System.Collections.Generic;

namespace DtPipe.Cli.Pipeline;

public record ParsedPipeline(GlobalOptions Globals, IReadOnlyList<BranchSpec> Branches);

public record GlobalOptions
{
    // Strictly global flags (structural — read by PipelineLexer/Converter only)
    public int DryRunCount { get; init; }
    public bool NoStats { get; init; }
    public string? LogPath { get; init; }
    public string? JobFile { get; init; }
    public string? ExportJobFile { get; init; }
    public bool IgnoreNulls { get; init; }
    public string? DryRunInteractiveBranch { get; set; }

    /// <summary>All raw flag values (key→value) for passthrough to PipelineToJobConverter.</summary>
    public IReadOnlyDictionary<string, object?> AllFlags { get; init; } = new Dictionary<string, object?>(System.StringComparer.OrdinalIgnoreCase);
}

/// <summary>
/// Per-branch data extracted by PipelineLexer.
/// Contains only DAG-routing fields and stage-scoped raw args.
/// All adapter/engine flags flow exclusively through RawArgs → FlagBinder → adapter options.
/// </summary>
public record BranchSpec
{
    // DAG routing (structural — lexer splitting logic depends on these)
    public string? Alias { get; init; }
    public string? Input { get; init; }
    public string? Output { get; init; }
    public List<string> From { get; init; } = new();
    public List<string> Ref { get; init; } = new();

    // Stage-scoped args — set by PipelineLexer, used by ProviderConfigurationService
    // ReaderArgs   : flags from start up to first Pipeline-stage trigger or -o
    // PipelineArgs : flags from first Pipeline-stage trigger to -o (transformer scope)
    // WriterArgs   : flags from -o to end
    public string[] ReaderArgs   { get; init; } = System.Array.Empty<string>();
    public string[] PipelineArgs { get; init; } = System.Array.Empty<string>();
    public string[] WriterArgs   { get; init; } = System.Array.Empty<string>();

    // Full flat args (union of the three above — kept for components that iterate all tokens)
    public string[] RawArgs { get; init; } = System.Array.Empty<string>();
    public IReadOnlyDictionary<string, List<string>> Flags { get; init; } = new Dictionary<string, List<string>>(System.StringComparer.OrdinalIgnoreCase);
}

