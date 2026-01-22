using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Oracle;

public class OracleReaderFactory : BaseCliContributor, IStreamReaderFactory
{
    public OracleReaderFactory(OptionsRegistry registry) : base(registry) { }

    public override string ProviderName => "oracle";
    public override string Category => "Reader Options";

    public bool CanHandle(string connectionString)
    {
        return OracleConnectionHelper.CanHandle(connectionString); 
    }

    public IStreamReader Create(DumpOptions options)
    {
        return new OracleStreamReader(
            OracleConnectionHelper.GetConnectionString(options.ConnectionString), 
            options.Query,
            Registry.Get<OracleOptions>(),
            options.QueryTimeout);
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<OracleStreamReader>();
    }
}
