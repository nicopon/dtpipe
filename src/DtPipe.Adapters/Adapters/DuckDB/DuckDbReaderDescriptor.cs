using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.DuckDB;

public class DuckDbReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => "duck";
    public string Category => "Reader Options";
    public Type OptionsType => typeof(DuckDbReaderOptions);
    public bool CanHandle(string connectionString) => connectionString.Contains(".duckdb", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (DuckDbReaderOptions)options;
        return new DuckDataSourceReader(DuckDbConnectionHelper.GetConnectionString(connectionString), opt.Query ?? "SELECT 1", opt);
    }
}
