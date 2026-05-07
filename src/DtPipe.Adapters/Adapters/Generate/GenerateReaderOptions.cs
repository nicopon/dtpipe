using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Generate;

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
