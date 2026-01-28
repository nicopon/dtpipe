using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;
using Microsoft.Extensions.DependencyInjection;

namespace QueryDump.Adapters.Sqlite;

public class SqliteWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ProviderName => "sqlite";

    public Type OptionsType => typeof(SqliteWriterOptions);

    public bool RequiresQuery => false;

    public bool CanHandle(string connectionString)
    {
        return SqliteConnectionHelper.CanHandle(connectionString);
    }

    public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        var registry = serviceProvider.GetRequiredService<OptionsRegistry>();
        // Ensure registry has options if not present (logic from original factory)
        if (!registry.Has<SqliteWriterOptions>())
        {
            registry.Register((SqliteWriterOptions)options);
        }
        
        
        var dsConnectionString = SqliteConnectionHelper.ToDataSourceConnectionString(connectionString);
        return new SqliteDataWriter(dsConnectionString, registry);
    }
}
