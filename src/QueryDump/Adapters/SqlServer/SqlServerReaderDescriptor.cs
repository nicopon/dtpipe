using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.SqlServer;

public class SqlServerReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ProviderName => SqlServerConstants.ProviderName;

    public Type OptionsType => typeof(SqlServerReaderOptions);

    public bool RequiresQuery => true;

    public bool CanHandle(string connectionString)
    {
        return SqlServerConnectionHelper.CanHandle(connectionString);
    }

    public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        return new SqlServerStreamReader(
            SqlServerConnectionHelper.GetConnectionString(connectionString),
            context.Query!,
            (SqlServerReaderOptions)options,
            context.QueryTimeout);
    }
}
