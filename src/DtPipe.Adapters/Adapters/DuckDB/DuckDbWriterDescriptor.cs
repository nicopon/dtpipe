using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.DuckDB;

public class DuckDbWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => "duck";
    public string Category => "Writer Options";
    public Type OptionsType => typeof(DuckDbWriterOptions);
    public bool CanHandle(string connectionString) => connectionString.Contains(".duckdb", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => true;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (DuckDbWriterOptions)options;
        return new DuckDbDataWriter(DuckDbConnectionHelper.GetConnectionString(connectionString), opt, serviceProvider.GetRequiredService<ILogger<DuckDbDataWriter>>(), new DuckDbTypeConverter());
    }
}
