using DtPipe.Core.Options;

namespace DtPipe.Adapters.Generate;

public record GenerateReaderOptions : IProviderOptions
{
	public static string Prefix => GenerateConstants.ProviderName;
	public static string DisplayName => "Data Generator";

	public long RowCount { get; set; } = 100;
	public int? RowsPerSecond { get; set; }
}
