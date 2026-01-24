using QueryDump.Core.Options;

namespace QueryDump.Adapters.Checksum;

public class ChecksumOptions : IProviderOptions
{
    public static string Prefix => "checksum";
    public static string DisplayName => "Checksum Verifier";

    public string OutputPath { get; set; } = "";
}
