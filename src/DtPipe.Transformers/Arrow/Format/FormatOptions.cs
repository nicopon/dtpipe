using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Arrow.Format;

[Description("Builds column values by substituting other columns into a '{COLUMN}' placeholder template string.")]
[ComponentHelp(
    usageNotes: "In YAML, use the 'mappings' section where the key is the target column (existing or new) and the value is a template with '{COLUMN}' or '{COLUMN:format}' placeholders (the format part uses .NET format specifiers). Use the 'options' block for 'skip-null' to skip when all referenced columns are null.",
    examples: new[] {
        "transformers:\n  - type: format\n    mappings:\n      display_name: \"{first_name} {last_name}\"\n      date_fr: \"{created_at:dd/MM/yyyy}\""
    })]
public class FormatOptions : ITransformerOptions
{
    public static string Prefix => "format";
    public static string DisplayName => "Format/Template Transformer";

    [ComponentOption(Description = "Target:Template mapping with optional format specifiers (repeatable, e.g. 'DATE_FR:{DATE:dd/MM/yyyy}' or 'FULL:{FIRST} {LAST}')")]
    public IEnumerable<string> Format { get; set; } = Array.Empty<string>();

    [ComponentOption(Description = "Skip format when all referenced source columns are null")]
    public bool SkipNull { get; set; } = false;
}
