using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Sqlite;

public class SqliteReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => SqliteMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(SqliteReaderOptions);
    public bool CanHandle(string connectionString) => SqliteMetadata.CanHandle(connectionString);
    public bool SupportsStdio => SqliteMetadata.SupportsStdio;
    public bool RequiresQuery => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (SqliteReaderOptions)options;
        return new SqliteStreamReader(SqliteConnectionHelper.ToDataSourceConnectionString(connectionString), opt.Query ?? "SELECT 1", 0);
    }
}
