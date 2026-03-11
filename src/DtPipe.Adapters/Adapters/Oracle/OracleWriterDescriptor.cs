using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Oracle;

public class OracleWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => OracleMetadata.ComponentName;
    public string Category => "Writer Options";
    public Type OptionsType => typeof(OracleWriterOptions);
    public bool CanHandle(string connectionString) => OracleMetadata.CanHandle(connectionString);
    public bool SupportsStdio => OracleMetadata.SupportsStdio;
    public bool RequiresQuery => true;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (OracleWriterOptions)options;
        return new OracleDataWriter(connectionString, opt, serviceProvider.GetRequiredService<ILogger<OracleDataWriter>>(), new OracleTypeConverter());
    }
}
