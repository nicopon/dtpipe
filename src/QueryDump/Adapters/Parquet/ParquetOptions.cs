using QueryDump.Core.Options;

namespace QueryDump.Adapters.Parquet;

public record ParquetOptions : IWriterOptions
{
    public static string Prefix => "pq";
    public static string DisplayName => "Parquet Writer";
    
    // Placeholder. In future we could add CompressionMethod (Snappy, Gzip, etc.)
}
