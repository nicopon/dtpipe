using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Parquet;

[Description("Writes data to a Parquet file.")]
[ComponentHelp(
    usageNotes: "Connection string is a file path ending in '.parquet' (or the 'parquet:' prefix), or '-' for stdout when redirected. If the path is a directory or has no extension, the writer appends 'export.parquet' or a '.parquet' suffix automatically.",
    examples: new[] {
        "main:\n  input: \"<adapter-prefix>:<source>\"\n  output: \"data.parquet\""
    })]
public record ParquetWriterOptions : IWriterOptions
{
    public static string Prefix => ParquetConstants.ProviderName;
    public static string DisplayName => "Parquet Writer";

    // Placeholder. In future we could add CompressionMethod (Snappy, Gzip, etc.)
}
