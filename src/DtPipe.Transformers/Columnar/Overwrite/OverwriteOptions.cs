using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Columnar.Overwrite;

public class OverwriteOptions : ITransformerOptions
{
	public static string Prefix => "overwrite";
	public static string DisplayName => "Static Overwrite Transformer";

	[ComponentOption(Description = "Column:value mapping to overwrite with static value (repeatable)")]
	public IEnumerable<string> Overwrite { get; set; } = Array.Empty<string>();

	[ComponentOption(Description = "Skip overwrite when source value is null")]
	public bool SkipNull { get; set; } = false;
}
