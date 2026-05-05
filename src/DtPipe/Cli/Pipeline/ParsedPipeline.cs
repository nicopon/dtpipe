using System.Collections.Generic;

namespace DtPipe.Cli.Pipeline;

public record ParsedPipeline(GlobalOptions Globals, IReadOnlyList<BranchSpec> Branches);

public record GlobalOptions
{
    // Strictly global flags
    public int DryRunCount { get; init; }
    public bool NoStats { get; init; }
    public string? LogPath { get; init; }
    public string? JobFile { get; init; }
    public string? ExportJobFile { get; init; }
    public string? MetricsPath { get; init; }

    // Overridable defaults propagated to all branches
    public string? Key { get; init; }
    public int Limit { get; init; }
    public int BatchSize { get; init; }
    public double SamplingRate { get; init; } = 1.0;
    public int? SamplingSeed { get; init; }
    public string? Prefix { get; init; }

    public IReadOnlyDictionary<string, object?> AllFlags { get; init; } = new Dictionary<string, object?>(System.StringComparer.OrdinalIgnoreCase);
}

public record BranchSpec
{
    public string? Alias { get; init; }
    public string? Input { get; init; }
    public string? Output { get; init; }
    public string? Query { get; init; }
    public string? Table { get; init; }
    public List<string> From { get; init; } = new();
    public List<string> Ref { get; init; } = new();
    public string? Strategy { get; init; }
    public string? InsertMode { get; init; }
    public string? SchemaSave { get; init; }
    public string? SchemaLoad { get; init; }
    public string? Path { get; init; }
    public string? ColumnTypes { get; init; }
    public bool AutoColumnTypes { get; init; }
    public int MaxSample { get; init; }
    public string? Encoding { get; init; }
    public int ConnectionTimeout { get; init; }
    public int QueryTimeout { get; init; }
    public bool UnsafeQuery { get; init; }
    public bool StrictSchema { get; init; }
    public bool NoSchemaValidation { get; init; }
    public bool AutoMigrate { get; init; }
    public string? PreExec { get; init; }
    public string? PostExec { get; init; }
    public string? OnErrorExec { get; init; }
    public string? FinallyExec { get; init; }

    // Overrides
    public string? Key { get; init; }
    public int Limit { get; init; }
    public int BatchSize { get; init; }
    public double SamplingRate { get; init; } = 1.0;
    public int? SamplingSeed { get; init; }
    public string? Prefix { get; init; }
    public string? LogPath { get; init; }
    public string? MetricsPath { get; init; }

    public string[] RawArgs { get; init; } = System.Array.Empty<string>();
    public IReadOnlyDictionary<string, List<string>> Flags { get; init; } = new Dictionary<string, List<string>>(System.StringComparer.OrdinalIgnoreCase);
}
