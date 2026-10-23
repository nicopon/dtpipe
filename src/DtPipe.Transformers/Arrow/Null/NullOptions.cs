using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Arrow.Null;

[Description("Sets specified columns to null, clearing their existing values.")]
[ComponentHelp(
    usageNotes: "In YAML, use the 'mappings' section where each key is a column name to null out; the value is ignored.",
    examples: new[] {
        "transformers:\n  - type: null\n    mappings:\n      phone: ~\n      ssn: ~"
    })]
public class NullOptions : ITransformerOptions
{
    public static string Prefix => "null";
    public static string DisplayName => "Null Transformer";

    [ComponentOption("--null", Description = "Column(s) to set to null (repeatable)")]
    public IEnumerable<string> Columns { get; set; } = Array.Empty<string>();
}
