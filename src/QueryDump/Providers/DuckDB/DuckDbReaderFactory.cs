using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.DuckDB;

public class DuckDbReaderFactory : BaseCliContributor, IStreamReaderFactory
{
    public DuckDbReaderFactory(OptionsRegistry registry) : base(registry) { }

    public override string ProviderName => "duckdb";
    public override string Category => "Reader Options";

    public bool CanHandle(string connectionString)
    {
        return DuckDbConnectionHelper.CanHandle(connectionString);
    }

    public IStreamReader Create(DumpOptions options)
    {
        var connectionString = DuckDbConnectionHelper.GetConnectionString(options.ConnectionString);

        if (!connectionString.Contains("DataSource=", StringComparison.OrdinalIgnoreCase) 
            && !connectionString.StartsWith("Data Source=", StringComparison.OrdinalIgnoreCase))
        {
            connectionString = $"Data Source={connectionString}";
        }

        return new DuckDataSourceReader(
            connectionString,
            options.Query,
            Registry.Get<DuckDbOptions>(),
            options.QueryTimeout);
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<DuckDataSourceReader>();
    }
}
