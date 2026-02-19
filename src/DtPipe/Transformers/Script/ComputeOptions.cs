using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Script;

public record ComputeOptions : ITransformerOptions
{
	public static string Prefix => "compute";
	public static string DisplayName => "Compute (JS)";

	[CliOption(Description = "Column:script mapping (e.g. TITLE:row.TITLE.substring(0,5))", Aliases = new[] { "--script" })]
	public IReadOnlyList<string> Compute { get; init; } = [];

	[CliOption(Description = "Skip script execution when input value is null", Aliases = new[] { "--script-skip-null" })]
	public bool SkipNull { get; init; } = false;

	public Dictionary<string, string> ComputeTypes { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}
