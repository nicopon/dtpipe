using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.SqlServer;

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
