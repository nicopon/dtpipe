using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Sqlite;

public class SqliteReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => "sqlite";
    public string Category => "Reader Options";
    public Type OptionsType => typeof(SqliteReaderOptions);
    public bool CanHandle(string connectionString) => connectionString.EndsWith(".db", StringComparison.OrdinalIgnoreCase) || connectionString.EndsWith(".sqlite", StringComparison.OrdinalIgnoreCase) || connectionString.Contains("Data Source=", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (SqliteReaderOptions)options;
        return new SqliteStreamReader(SqliteConnectionHelper.ToDataSourceConnectionString(connectionString), opt.Query ?? "SELECT 1", 0);
    }
}
