using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.DuckDB;

public class DuckDataSourceReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => DuckDbMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(DuckDbReaderOptions);
    public bool CanHandle(string connectionString) => DuckDbMetadata.CanHandle(connectionString);
    public bool SupportsStdio => DuckDbMetadata.SupportsStdio;
    public bool RequiresQuery => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (DuckDbReaderOptions)options;
        var logger = serviceProvider.GetService<ILogger<DuckDataSourceReader>>();
        return new DuckDataSourceReader(connectionString, opt.Query ?? "SELECT 1", opt, logger);
    }
}
