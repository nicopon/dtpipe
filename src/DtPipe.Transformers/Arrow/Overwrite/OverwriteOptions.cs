using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Arrow.Overwrite;

[Description("Overwrites specified columns with a fixed static value for every row.")]
[ComponentHelp(
	usageNotes: "In YAML, use the 'mappings' section where the key is the column name and the value is the static replacement (the overwritten column is emitted as a string); use the 'options' block for 'skip-null' to leave null source values untouched. A mapping whose column the incoming rows do NOT carry CREATES it, as a string appended after the source columns, so a constant column costs no row-mode detour; 'skip-null' has nothing to skip on a created column.",
	examples: new[] {
		"transformers:\n  - type: overwrite\n    mappings:\n      status: Active\n      region: EU\n    options:\n      skip-null: true"
	})]
public class OverwriteOptions : ITransformerOptions
{
	public static string Prefix => "overwrite";
	public static string DisplayName => "Static Overwrite Transformer";

	[ComponentOption(Description = "Column:value mapping to overwrite with static value (repeatable)")]
	public IEnumerable<string> Overwrite { get; set; } = Array.Empty<string>();

	[ComponentOption(Description = "Skip overwrite when source value is null")]
	public bool SkipNull { get; set; } = false;
}
