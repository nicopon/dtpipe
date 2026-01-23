using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;
using Microsoft.Extensions.DependencyInjection;

namespace QueryDump.Adapters.Sqlite;

public class SqliteWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ProviderName => "sqlite-writer";

    public Type OptionsType => typeof(SqliteWriterOptions);

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
        
        // Note: The original logic in factory constructor registered default options if missing. 
        // Here we receive 'options' which is already instance of SqliteWriterOptions (from CLI binding or default).
        // BUT the Writer constructor takes the whole Registry.
        // We might need to ensure the specific instance 'options' is in the registry?
        // Or just pass the registry as is.
        // If the Writer retrieves options via registry.Get<SqliteWriterOptions>(), it needs to be the SAME instance that was bound?
        // Actually, CliProviderFactory binds options to registry BEFORE calling Create.
        // So registry already contains the bound options if passing through CLI flow.
        // So just passing registry is fine.
        
        var dsConnectionString = SqliteConnectionHelper.ToDataSourceConnectionString(connectionString);
        return new SqliteDataWriter(dsConnectionString, registry);
    }
}
