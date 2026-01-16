using QueryDump.Core.Options;

namespace QueryDump.Writers.Parquet;

public record ParquetOptions : IWriterOptions
{
    public static string Prefix => "pq";
    
    // Placeholder. In future we could add CompressionMethod (Snappy, Gzip, etc.)
}
