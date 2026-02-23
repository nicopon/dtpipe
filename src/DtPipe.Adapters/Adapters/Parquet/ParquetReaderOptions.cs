using DtPipe.Core.Options;

namespace DtPipe.Adapters.Parquet;

public record ParquetReaderOptions : IProviderOptions
{
    public static string Prefix => ParquetConstants.ProviderName;
    public static string DisplayName => "Parquet Reader";
}
