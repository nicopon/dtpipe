using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Checksum;

public class ChecksumWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => ChecksumMetadata.ComponentName;
    public string Category => "Writer Options";
    public Type OptionsType => typeof(ChecksumWriterOptions);
    public bool CanHandle(string connectionString) => ChecksumMetadata.CanHandle(connectionString);
    public bool SupportsStdio => ChecksumMetadata.SupportsStdio;
    public bool RequiresQuery => false;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (ChecksumWriterOptions)options;
        return new ChecksumDataWriter(connectionString, opt, serviceProvider.GetRequiredService<ILogger<ChecksumDataWriter>>());
    }
}
