using DtPipe.Cli.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Null;

public class NullOptions : ITransformerOptions
{
	public static string Prefix => "null";
	public static string DisplayName => "Null Transformer";

	[CliOption("--null", Description = "Column(s) to set to null (repeatable)")]
	public IEnumerable<string> Columns { get; set; } = Array.Empty<string>();
}
