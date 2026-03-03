using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.JsonL;

public class JsonLWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => JsonLMetadata.ComponentName;
    public string Category => "Writer Options";
    public Type OptionsType => typeof(JsonLWriterOptions);
    public bool CanHandle(string connectionString) => JsonLMetadata.CanHandle(connectionString);
    public bool SupportsStdio => JsonLMetadata.SupportsStdio;
    public bool RequiresQuery => false;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (JsonLWriterOptions)options;
        return new JsonLDataWriter(connectionString, opt);
    }
}
