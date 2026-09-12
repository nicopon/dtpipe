using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Row.Expand;

[Description("Expands a single row into multiple rows using a JavaScript expression that returns an array of row objects.")]
[ComponentHelp(
	usageNotes: "In YAML, place the JavaScript expression as a 'mappings' key with an empty value, same convention as 'filter'. "
		+ "The expression is evaluated over 'row' and must return an ARRAY OF OBJECTS; each element becomes one output row, and an "
		+ "array of plain values ('row.tags') is refused rather than silently producing nothing. "
		+ "The output schema is the source schema: a key the source does not carry — the 'tag' below — reaches the output only if it "
		+ "is declared with 'expand-types' ('--expand-types' on the command line), as name:type with type defaulting to string. "
		+ "Accepted types: string, int, long, double, decimal, bool, datetime, guid.",
	examples: new[] {
		"transformers:\n  - type: expand\n    mappings:\n      \"row.tags.map(t => ({ ...row, tag: t }))\": \"\"\n    options:\n      expand-types: \"tag:string\""
	})]
public class ExpandOptions : ITransformerOptions
{
	public static string Prefix => "expand";
	public static string DisplayName => "Expand Options";

	[ComponentOption("--expand", Description = "A JavaScript expression over 'row' returning an array of row objects. Each element becomes a new row.")]
	public string[]? Expand { get; set; }

	/// <summary>
	/// Columns the expression creates, as name → type hint.
	/// </summary>
	/// <remarks>
	/// A key discovered while transforming arrives too late: <c>InitializeAsync</c> fixes the
	/// schema before the first row, so a column the expression invents has to be declared to
	/// exist at all. Nothing fed this before — no flag, no YAML key — and the expression's new
	/// keys were dropped, which made the shipped example produce every column but the one it was
	/// written to show.
	/// </remarks>
	[ComponentOption("--expand-types", Description = "Explicit type for a column the expression creates, as name:type (type defaults to string). Repeat for several.")]
	public Dictionary<string, string> ExpandTypes { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}
