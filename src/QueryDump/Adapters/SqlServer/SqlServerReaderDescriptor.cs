using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.SqlServer;

public class SqlServerReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ProviderName => "mssql";

    public Type OptionsType => typeof(SqlServerOptions);

    public bool CanHandle(string connectionString)
    {
        return SqlServerConnectionHelper.CanHandle(connectionString);
    }

    public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        return new SqlServerStreamReader(
            SqlServerConnectionHelper.GetConnectionString(connectionString),
            context.Query,
            context.QueryTimeout);
    }
}
