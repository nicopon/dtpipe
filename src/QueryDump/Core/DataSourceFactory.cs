using QueryDump.Configuration;
using QueryDump.Core.Options;
using QueryDump.Providers.Oracle;
using QueryDump.Providers.SqlServer;
using QueryDump.Providers.DuckDB;

namespace QueryDump.Core;

public interface IDataSourceFactory
{
    IDataSourceReader Create(DumpOptions options);
    IEnumerable<Type> GetSupportedOptionTypes();
}



public class DataSourceFactory : IDataSourceFactory
{
    private readonly OptionsRegistry _registry;

    public DataSourceFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<OracleStreamReader>();
        yield return ComponentOptionsHelper.GetOptionsType<SqlServerStreamReader>();
        yield return ComponentOptionsHelper.GetOptionsType<DuckDataSourceReader>();
    }

    public IDataSourceReader Create(DumpOptions options)
    {
        return options.Provider.ToLowerInvariant() switch
        {
            "oracle" => new OracleStreamReader(
                options.ConnectionString, 
                options.Query,
                _registry.Get<OracleOptions>(),
                options.QueryTimeout),
            "sqlserver" => new SqlServerStreamReader(
                options.ConnectionString,
                options.Query,
                _registry.Get<SqlServerOptions>(),
                options.QueryTimeout),
            "duckdb" => new DuckDataSourceReader(
                 options.ConnectionString,
                 options.Query,
                 _registry.Get<DuckDbOptions>(),
                 options.QueryTimeout),
            _ => throw new ArgumentException($"Unsupported provider: {options.Provider}")
        };
    }
}
