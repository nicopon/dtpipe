using DtPipe.Core.Options;

namespace DtPipe.Adapters.Checksum;

public record ChecksumWriterOptions : IWriterOptions
{
	public static string Prefix => ChecksumConstants.ProviderName;
	public static string DisplayName => "Checksum Verifier";

	public string OutputPath { get; set; } = "";
}
