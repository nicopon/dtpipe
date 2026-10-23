using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Parquet;

[Description("Reads data from a Parquet file.")]
[ComponentHelp(
    usageNotes: "Connection string is a file path ending in '.parquet' (or the 'parquet:' prefix). Parquet's footer requires a seekable stream, so — unlike the other file adapters — reading from stdin is not supported; always point to a real file.",
    examples: new[] {
        "main:\n  input: \"data.parquet\"\n  output: \"<adapter-prefix>:<target>\""
    })]
public record ParquetReaderOptions : IProviderOptions
{
    public static string Prefix => ParquetConstants.ProviderName;
    public static string DisplayName => "Parquet Reader";
}
