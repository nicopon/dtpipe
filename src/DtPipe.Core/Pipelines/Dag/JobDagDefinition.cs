using System.Text.Json;
using System.Text.Json.Serialization;

namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Represents the complete Directed Acyclic Graph (DAG) for a multi-branch pipeline job.
/// </summary>
public record JobDagDefinition
{
    /// <summary>
    /// The ordered list of branches that make up the DAG.
    /// Branches are typically executed in parallel but may have logical data dependencies
    /// (e.g., a processor branch consuming data from upstream branches).
    /// </summary>
    public IReadOnlyList<BranchDefinition> Branches { get; init; } = Array.Empty<BranchDefinition>();

    /// <summary>
    /// Gets a value indicating whether this job requires DAG orchestration
    /// (i.e., it contains more than one branch or an explicit processor).
    /// </summary>
    public bool IsDag => Branches.Count > 1 || Branches.Any(b => b.IsProcessor);

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    /// <summary>Serializes this definition to JSON.</summary>
    public string ToJson() => JsonSerializer.Serialize(this, _jsonOptions);

    /// <summary>Deserializes a <see cref="JobDagDefinition"/> from JSON.</summary>
    public static JobDagDefinition? FromJson(string json) =>
        JsonSerializer.Deserialize<JobDagDefinition>(json, _jsonOptions);
}
