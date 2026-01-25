using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.SqlServer;

public class SqlServerWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ProviderName => "mssql";
    public Type OptionsType => typeof(SqlServerWriterOptions);

    public bool CanHandle(string connectionString)
    {
        return connectionString.StartsWith("sqlserver:", StringComparison.OrdinalIgnoreCase);
    }

    public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        var sqlOptions = (SqlServerWriterOptions)options;
        // Strip prefix
        var conn = connectionString.StartsWith("sqlserver:") ? connectionString.Substring(10) : connectionString;
        return new SqlServerDataWriter(conn, sqlOptions);
    }
}
