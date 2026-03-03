using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.DuckDB;

public class DuckDbWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => DuckDbMetadata.ComponentName;
    public string Category => "Writer Options";
    public Type OptionsType => typeof(DuckDbWriterOptions);
    public bool CanHandle(string connectionString) => DuckDbMetadata.CanHandle(connectionString);
    public bool SupportsStdio => DuckDbMetadata.SupportsStdio;
    public bool RequiresQuery => true;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (DuckDbWriterOptions)options;
        return new DuckDbDataWriter(DuckDbConnectionHelper.GetConnectionString(connectionString), opt, serviceProvider.GetRequiredService<ILogger<DuckDbDataWriter>>(), new DuckDbTypeConverter());
    }
}
