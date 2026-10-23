using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Arrow.Mask;

[Description("Masks sensitive columns using custom character patterns.")]
[ComponentHelp(
    usageNotes: "In YAML, use the 'mappings' section where the key is the column name and the value is the mask pattern ('#' keeps the character, other characters are replaced). If the value is empty, a default 15-asterisk mask is applied.",
    examples: new[] {
        "transformers:\n  - type: mask\n    mappings:\n      phone: \"###-###-####\"\n      SSN: \"\""
    })]
public class MaskOptions : ITransformerOptions
{
    public static string Prefix => "mask";
    public static string DisplayName => "Mask Transformer";

    [ComponentOption(Description = "Mask column. Format: COLUMN:pattern (# = keep, other = replace) or simply COLUMN for default full mask (15 asterisks).")]
    public IEnumerable<string> Mask { get; set; } = [];

    [ComponentOption(Description = "Skip mask when source value is null")]
    public bool SkipNull { get; set; } = false;
}
