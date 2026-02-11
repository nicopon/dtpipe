using DtPipe.Core.Options;

namespace DtPipe.Adapters.Sample;

public record SampleReaderOptions : IProviderOptions
{
	public static string Prefix => SampleConstants.ProviderName;
	public static string DisplayName => "Sample Data Generator";

	public long RowCount { get; set; } = 100;
}
