using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Writers.Csv;

namespace QueryDump.Writers.Csv;

public class CsvWriterFactory : BaseCliContributor, IDataWriterFactory
{
    public CsvWriterFactory(OptionsRegistry registry) : base(registry) { }

    public string SupportedExtension => ".csv";
    public override string ProviderName => "csv-writer";
    public override string Category => "Writer Options";

    public IDataWriter Create(DumpOptions options)
    {
        return new CsvDataWriter(options.OutputPath, Registry.Get<CsvOptions>());
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<CsvDataWriter>();
    }
}
