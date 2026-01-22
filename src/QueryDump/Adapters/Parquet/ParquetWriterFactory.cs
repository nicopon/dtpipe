using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Adapters.Parquet;

namespace QueryDump.Adapters.Parquet;

public class ParquetWriterFactory : BaseCliContributor, IDataWriterFactory
{
    public ParquetWriterFactory(OptionsRegistry registry) : base(registry) { }

    public string SupportedExtension => ".parquet";
    public override string ProviderName => "parquet-writer";
    public override string Category => "Writer Options";

    public IDataWriter Create(DumpOptions options)
    {
        return new ParquetDataWriter(options.OutputPath);
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<ParquetDataWriter>();
    }
}
