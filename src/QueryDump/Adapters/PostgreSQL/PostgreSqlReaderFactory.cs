using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.PostgreSQL;

public class PostgreSqlReaderFactory : BaseCliContributor, IStreamReaderFactory
{
    public PostgreSqlReaderFactory(OptionsRegistry registry) : base(registry) { }

    public override string ProviderName => "postgresql";
    public override string Category => "Reader Options";

    public bool CanHandle(string connectionString)
    {
        return PostgreSqlConnectionHelper.CanHandle(connectionString);
    }

    public IStreamReader Create(DumpOptions options)
    {
        return new PostgreSqlReader(
            PostgreSqlConnectionHelper.GetConnectionString(options.ConnectionString),
            options.Query,
            options.QueryTimeout
        );
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        return Enumerable.Empty<Type>();
    }
}
