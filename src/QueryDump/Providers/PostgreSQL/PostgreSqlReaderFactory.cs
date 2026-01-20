using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.PostgreSQL;

public class PostgreSqlReaderFactory : BaseCliContributor, IStreamReaderFactory
{
    public PostgreSqlReaderFactory(OptionsRegistry registry) : base(registry) { }

    public override string ProviderName => "postgresql";
    public override string Category => "PostgreSQL Options";

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
        yield return typeof(PostgreSqlOptions);
    }
}
