using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.SqlServer;

public class SqlServerReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => "mssql";
    public string Category => "Reader Options";
    public Type OptionsType => typeof(SqlServerReaderOptions);
    public bool CanHandle(string connectionString) => connectionString.StartsWith("sqlserver:", StringComparison.OrdinalIgnoreCase) || connectionString.Contains("Data Source=", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (SqlServerReaderOptions)options;
        return new SqlServerStreamReader(connectionString, opt.Query ?? "SELECT 1", opt, 0);
    }
}
