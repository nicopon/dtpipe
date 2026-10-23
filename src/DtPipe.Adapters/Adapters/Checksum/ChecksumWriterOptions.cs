using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Checksum;

[Description("Computes a running SHA-256 checksum over all rows written, used to verify that data survived a pipeline unchanged.")]
[ComponentHelp(
    usageNotes: "Connection string is a file path (e.g. 'checksum:out.checksum', or any path ending in '.checksum'); use '-' or an empty path to print the hash to stdout instead. The hash chains SHA-256 over each row's canonicalized values in arrival order, so it is also sensitive to row reordering — handy for comparing two pipeline runs of the same logical dataset.",
    examples: new[] {
        "main:\n  input: \"<adapter-prefix>:<source>\"\n  output: \"checksum:orders.checksum\""
    })]
public record ChecksumWriterOptions : IWriterOptions
{
    public static string Prefix => ChecksumConstants.ProviderName;
    public static string DisplayName => "Checksum Verifier";

    public string OutputPath { get; set; } = "";
}
