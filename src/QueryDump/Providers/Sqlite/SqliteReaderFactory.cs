using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.Sqlite;

public class SqliteReaderFactory : BaseCliContributor, IStreamReaderFactory
{
    public SqliteReaderFactory(OptionsRegistry registry) : base(registry) { }

    public override string ProviderName => "sqlite";
    public override string Category => "Reader Options";

    public bool CanHandle(string connectionString)
    {
        return SqliteConnectionHelper.CanHandle(connectionString);
    }

    public IStreamReader Create(DumpOptions options)
    {
        var connectionString = SqliteConnectionHelper.ToDataSourceConnectionString(options.ConnectionString);

        return new SqliteStreamReader(
            connectionString,
            options.Query,
            options.QueryTimeout);
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield break; // No specific reader options for SQLite for now
    }
}
