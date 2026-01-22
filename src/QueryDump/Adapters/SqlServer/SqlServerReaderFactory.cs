using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.SqlServer;

public class SqlServerReaderFactory : BaseCliContributor, IStreamReaderFactory
{
    public SqlServerReaderFactory(OptionsRegistry registry) : base(registry) { }

    public override string ProviderName => "sqlserver";
    public override string Category => "Reader Options";

    public bool CanHandle(string connectionString)
    {
        return SqlServerConnectionHelper.CanHandle(connectionString);
    }

    public IStreamReader Create(DumpOptions options)
    {
        return new SqlServerStreamReader(
            SqlServerConnectionHelper.GetConnectionString(options.ConnectionString),
            options.Query,
            options.QueryTimeout);
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<SqlServerStreamReader>();
    }
}
