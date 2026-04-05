using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.JsonL;

public class JsonLReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => JsonLMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(JsonLReaderOptions);
    public bool CanHandle(string connectionString) => JsonLMetadata.CanHandle(connectionString);
    public bool SupportsStdio => JsonLMetadata.SupportsStdio;
    public bool RequiresQuery => false;
    public bool YieldsColumnarOutput => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (JsonLReaderOptions)options;
        return new JsonLStreamReader(connectionString, opt, serviceProvider.GetRequiredService<ILogger<JsonLStreamReader>>());
    }
}
