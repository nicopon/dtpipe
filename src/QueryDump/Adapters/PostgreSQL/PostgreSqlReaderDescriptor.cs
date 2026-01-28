using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.PostgreSQL;

public class PostgreSqlReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ProviderName => PostgreSqlConstants.ProviderName;

    public Type OptionsType => typeof(EmptyOptions);

    public bool RequiresQuery => true;

    public bool CanHandle(string connectionString)
    {
        return PostgreSqlConnectionHelper.CanHandle(connectionString);
    }

    public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        return new PostgreSqlReader(
            PostgreSqlConnectionHelper.GetConnectionString(connectionString),
            context.Query!,
            context.QueryTimeout
        );
    }
}
