using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Arrow.Fake;

[Description("Anonymizes columns using fakers (Bogus library).")]
[ComponentHelp(
	usageNotes: "In YAML, use the 'mappings' section where key is the column name and value is the Bogus dataset.method path (e.g. 'name.fullName', 'internet.email'). A mapping whose column does not exist in the incoming rows CREATES it: this transformer both anonymizes existing columns and synthesizes new ones, so a source carrying no columns of interest is still enough to produce a fully populated table.",
	examples: new[] {
		"transformers:\n  - type: fake\n    mappings:\n      Name: name.fullName\n      Email: internet.email\n    options:\n      locale: fr\n      seed: 42"
	})]
public record FakeOptions : ITransformerOptions
{
	public static string Prefix => "fake";
	public static string DisplayName => "Anonymization";

	[ComponentOption(Description = "Column:faker mapping (e.g. EMAIL:internet.email, NAME:name.fullName, EMAIL_ALT:internet.email#alt)")]
	public IReadOnlyList<string> Fake { get; init; } = [];

	[Description("Locale for fake data (en, fr, de, es, ja, zh_CN...)")]
	public string Locale { get; init; } = "en";

	[Description("Global seed for reproducible random fakes across all columns")]
	public int? Seed { get; init; } = null;

	[Description("Column(s) to use as seed (same value = same fake output, ideal for composite ID columns)")]
	public IReadOnlyList<string> SeedColumn { get; init; } = [];

	[Description("Row-index based deterministic mode (row N always gets same values)")]
	public bool SeedRow { get; init; } = false;

	[ComponentOption("--skip-null", Description = "Skip fake generation when source value is null")]
	public bool SkipNull { get; init; } = false;
}
