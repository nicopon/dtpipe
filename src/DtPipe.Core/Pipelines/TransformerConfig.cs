namespace DtPipe.Core.Pipelines;

public record TransformerConfig
{
	public required string Type { get; init; }

	/// <summary>
	/// Mapping property aliased to specific types for YAML deserialization convenience.
	/// </summary>
	public Dictionary<string, string>? Mappings { get; init; }

	public Dictionary<string, string>? Mask => Type == "mask" ? Mappings : null;
	public Dictionary<string, string>? Fake => Type == "fake" ? Mappings : null;
	public Dictionary<string, string>? Format => Type == "format" ? Mappings : null;
	public Dictionary<string, string>? Script => Type == "script" ? Mappings : null;
	public Dictionary<string, string>? Overwrite => Type == "overwrite" ? Mappings : null;

	public Dictionary<string, string>? Options { get; init; }
}
