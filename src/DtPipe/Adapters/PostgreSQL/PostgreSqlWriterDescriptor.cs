using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.PostgreSQL;

public class PostgreSqlWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ProviderName => PostgreSqlConstants.ProviderName;

    public Type OptionsType => typeof(PostgreSqlWriterOptions);

    public bool RequiresQuery => false;

    public bool CanHandle(string connectionString)
    {
        return PostgreSqlConnectionHelper.CanHandle(connectionString);
    }

    public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        return new PostgreSqlDataWriter(
            PostgreSqlConnectionHelper.GetConnectionString(connectionString),
            (PostgreSqlWriterOptions)options
        );
    }
}
