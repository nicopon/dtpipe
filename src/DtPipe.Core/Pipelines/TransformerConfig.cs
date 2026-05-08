namespace DtPipe.Core.Pipelines;

public record TransformerConfig
{
	public required string Type { get; init; }

	/// <summary>
	/// Mapping property aliased to specific types for YAML deserialization convenience.
	/// </summary>
	public Dictionary<string, string>? Mappings { get; init; }
	public Dictionary<string, string>? Options { get; init; }
}
