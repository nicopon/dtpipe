using QueryDump.Core;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Oracle;

public class OracleDataWriterFactory : BaseCliContributor, IDataWriterFactory
{
    public OracleDataWriterFactory(OptionsRegistry registry) : base(registry) { }

    public override string ProviderName => "oracle-writer";
    public override string Category => "Writer Options";
    public string SupportedExtension => ""; 

    public bool CanHandle(string outputPath)
    {
        return OracleConnectionHelper.CanHandle(outputPath);
    }

    public IDataWriter Create(DumpOptions options)
    {
        var connectionString = OracleConnectionHelper.GetConnectionString(options.OutputPath);
        return new OracleDataWriter(connectionString, Registry.Get<OracleWriterOptions>());
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return typeof(OracleWriterOptions); 
    }
}
