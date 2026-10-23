using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Arrow.Project;

[Description("Selects, drops, and renames columns to reshape the output schema.")]
[ComponentHelp(
    usageNotes: "In YAML, 'mappings' keys define the whitelist of columns to keep, in order (values are ignored); 'drop' and 'rename' are configured in the 'options' block as a single, optionally comma-separated string (e.g. 'rename: \"old1:new1,old2:new2\"'). An explicitly dropped column is removed even if it is also whitelisted.",
    examples: new[] {
        "transformers:\n  - type: project\n    mappings:\n      id: ~\n      name: ~\n      email: ~"
    })]
public class ProjectOptions : ITransformerOptions
{
    public static string Prefix => "project";
    public static string DisplayName => "Projection Transformer";

    [ComponentOption("--project", Description = "Keep only specified columns. repeatable.")]
    public IEnumerable<string> Project { get; set; } = Array.Empty<string>();

    [ComponentOption("--drop", Description = "Remove specified columns. repeatable.")]
    public IEnumerable<string> Drop { get; set; } = Array.Empty<string>();

    [ComponentOption("--rename", Description = "Rename columns (Old:New). repeatable.")]
    public IEnumerable<string> Rename { get; set; } = Array.Empty<string>();
}
