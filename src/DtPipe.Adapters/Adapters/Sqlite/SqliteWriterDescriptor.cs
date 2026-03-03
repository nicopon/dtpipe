using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Sqlite;

public class SqliteWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => SqliteMetadata.ComponentName;
    public string Category => "Writer Options";
    public Type OptionsType => typeof(SqliteWriterOptions);
    public bool CanHandle(string connectionString) => SqliteMetadata.CanHandle(connectionString);
    public bool SupportsStdio => SqliteMetadata.SupportsStdio;
    public bool RequiresQuery => true;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (SqliteWriterOptions)options;
        return new SqliteDataWriter(SqliteConnectionHelper.ToDataSourceConnectionString(connectionString), opt, serviceProvider.GetRequiredService<ILogger<SqliteDataWriter>>(), new SqliteTypeConverter());
    }
}
