using QueryDump.Core.Options;

namespace QueryDump.Adapters.Parquet;

public record ParquetWriterOptions : IWriterOptions
{
    public static string Prefix => ParquetConstants.ProviderName;
    public static string DisplayName => "Parquet Writer";
    
    // Placeholder. In future we could add CompressionMethod (Snappy, Gzip, etc.)
}
