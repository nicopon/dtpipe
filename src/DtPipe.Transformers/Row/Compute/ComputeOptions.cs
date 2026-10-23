using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Row.Compute;

[Description("Used to compute new columns or update existing ones using JavaScript expressions.")]
[ComponentHelp(
    usageNotes: "In YAML, use the 'mappings' section to specify column-to-script configurations. Values are evaluated as JavaScript expressions.",
    examples: new[] {
        "transformers:\n  - type: compute\n    mappings:\n      fullname: row.first_name + ' ' + row.last_name\n      age: parseInt(row.age) + 1"
    })]
public record ComputeOptions : ITransformerOptions
{
    public static string Prefix => "compute";
    public static string DisplayName => "Compute (JS)";

    [ComponentOption(Description = "Column:script mapping (e.g. TITLE:row.TITLE.substring(0,5))")]
    public IReadOnlyList<string> Compute { get; init; } = [];

    [ComponentOption("--skip-null", Description = "Skip script execution when input value is null")]
    public bool SkipNull { get; init; } = false;

    [ComponentOption("--compute-types", Description = "Explicit output type for computed columns (e.g. Col:int32). Repeatable.")]
    public Dictionary<string, string> ComputeTypes { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}
