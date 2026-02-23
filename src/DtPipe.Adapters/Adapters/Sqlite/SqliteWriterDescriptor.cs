using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Sqlite;

public class SqliteWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => "sqlite";
    public string Category => "Writer Options";
    public Type OptionsType => typeof(SqliteWriterOptions);
    public bool CanHandle(string connectionString) => connectionString.EndsWith(".db", StringComparison.OrdinalIgnoreCase) || connectionString.EndsWith(".sqlite", StringComparison.OrdinalIgnoreCase) || connectionString.Contains("Data Source=", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => true;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (SqliteWriterOptions)options;
        return new SqliteDataWriter(SqliteConnectionHelper.ToDataSourceConnectionString(connectionString), opt, serviceProvider.GetRequiredService<ILogger<SqliteDataWriter>>(), new SqliteTypeConverter());
    }
}
