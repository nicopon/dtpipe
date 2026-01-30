using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Configuration;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.PostgreSQL;

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
