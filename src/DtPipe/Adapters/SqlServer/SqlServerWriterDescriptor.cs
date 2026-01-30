using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Configuration;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.SqlServer;

public class SqlServerWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ProviderName => SqlServerConstants.ProviderName;
    public Type OptionsType => typeof(SqlServerWriterOptions);

    public bool RequiresQuery => false;

    public bool CanHandle(string connectionString)
    {
        return connectionString.StartsWith("mssql:", StringComparison.OrdinalIgnoreCase);
    }

    public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        var sqlOptions = (SqlServerWriterOptions)options;
        return new SqlServerDataWriter(connectionString, sqlOptions);
    }
}
