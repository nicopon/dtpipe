using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.SqlServer;

public class SqlServerWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => SqlServerMetadata.ComponentName;
    public string Category => "Writer Options";
    public Type OptionsType => typeof(SqlServerWriterOptions);
    public bool CanHandle(string connectionString) => SqlServerMetadata.CanHandle(connectionString);
    public bool SupportsStdio => SqlServerMetadata.SupportsStdio;
    public bool RequiresQuery => true;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (SqlServerWriterOptions)options;
        return new SqlServerDataWriter(connectionString, opt, serviceProvider.GetRequiredService<ILogger<SqlServerDataWriter>>(), new SqlServerTypeConverter());
    }
}
