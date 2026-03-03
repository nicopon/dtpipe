using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Columnar.Format;

public class FormatOptions : ITransformerOptions
{
	public static string Prefix => "format";
	public static string DisplayName => "Format/Template Transformer";

	[ComponentOption(Description = "Target:Template mapping with optional format specifiers (repeatable, e.g. 'DATE_FR:{DATE:dd/MM/yyyy}' or 'FULL:{FIRST} {LAST}')")]
	public IEnumerable<string> Format { get; set; } = Array.Empty<string>();

	[ComponentOption(Description = "Skip format when all referenced source columns are null")]
	public bool SkipNull { get; set; } = false;
}
