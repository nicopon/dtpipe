using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Row.Compute;

public record ComputeOptions : ITransformerOptions
{
	public static string Prefix => "compute";
	public static string DisplayName => "Compute (JS)";

	[ComponentOption(Description = "Column:script mapping (e.g. TITLE:row.TITLE.substring(0,5))")]
	public IReadOnlyList<string> Compute { get; init; } = [];

	[ComponentOption(Description = "Skip script execution when input value is null")]
	public bool SkipNull { get; init; } = false;

	[ComponentOption("--compute-types", Description = "Explicit output type for computed columns (e.g. Col:int32). Repeatable.")]
	public Dictionary<string, string> ComputeTypes { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}
