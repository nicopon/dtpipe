using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Transformers.Format;

public class FormatOptions : ITransformerOptions
{
    public static string Prefix => "format";
    public static string DisplayName => "Format/Template Transformer";

    [CliOption("--format", Description = "Target:Template mapping with optional format specifiers (repeatable, e.g. 'DATE_FR:{DATE:dd/MM/yyyy}' or 'FULL:{FIRST} {LAST}')")]
    public IEnumerable<string> Mappings { get; set; } = Array.Empty<string>();

    [CliOption("--format-skip-null", Description = "Skip format when all referenced source columns are null")]
    public bool SkipNull { get; set; } = false;
}
