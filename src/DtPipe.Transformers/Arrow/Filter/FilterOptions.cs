using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Arrow.Filter;

[Description("Filters rows using a JavaScript boolean expression.")]
[ComponentHelp(
    usageNotes: "In YAML, place filter expressions under the 'mappings' section (keys representing the boolean expressions).",
    examples: new[] {
        "transformers:\n  - type: filter\n    mappings:\n      \"parseInt(row.age) >= 18\": \"\"\n      \"row.country === 'France'\": \"\""
    })]
public class FilterOptions : ITransformerOptions
{
    public static string Prefix => "filter";
    public static string DisplayName => "Filter Options";

    [ComponentOption("--filter", Description = "Filter expression(s). Multiple filters are applied sequentially.")]
    public string[]? Filters { get; set; }
}
