using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Checksum;

public class ChecksumWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => "checksum";
    public string Category => "Writer Options";
    public Type OptionsType => typeof(ChecksumWriterOptions);
    public bool CanHandle(string connectionString) => connectionString.Equals("checksum:", StringComparison.OrdinalIgnoreCase) || connectionString.EndsWith(".sha256", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => false;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (ChecksumWriterOptions)options;
        return new ChecksumDataWriter(connectionString, opt, serviceProvider.GetRequiredService<ILogger<ChecksumDataWriter>>());
    }
}
