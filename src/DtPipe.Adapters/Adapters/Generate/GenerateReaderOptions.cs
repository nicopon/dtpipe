using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Generate;

[Description("Generates synthetic rows with a single incrementing GenerateIndex column, useful for testing and benchmarking pipelines.")]
[ComponentHelp(
	usageNotes: "Connection string format: 'generate:N' (row count, accepts 'k'/'m' suffixes, e.g. 'generate:10m') or 'generate:count=N;rate=R' to also throttle throughput. In YAML, use 'provider-options' -> 'generate' to set row-count, rows-per-second or arrow-batch-size explicitly. The YAML keys are the ones listed above, which are not always the CLI flag: the throttle is '--throttle' on the command line and 'rows-per-second' in YAML.",
	examples: new[] {
		"main:\n  input: \"generate:1m\"\n  provider-options:\n    generate:\n      rows-per-second: 50000\n  output: \"null:\""
	})]
public record GenerateReaderOptions : IProviderOptions
{
	public static string Prefix => GenerateConstants.ProviderName;
	public static string DisplayName => "Data Generator";

	[ComponentOption("--row-count", Aliases = new[] { "-r" }, Description = "Number of rows to generate")]
	public long RowCount { get; set; } = 100;

	[ComponentOption("--throttle", Description = "Rows per second to generate (throttle)")]
	public int? RowsPerSecond { get; set; }

	[ComponentOption("--arrow-batch-size", Description = "Size of each generated Arrow batch")]
	public int ArrowBatchSize { get; set; } = 100_000;
}
