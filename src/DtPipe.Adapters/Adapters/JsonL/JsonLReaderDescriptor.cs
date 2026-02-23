using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.JsonL;

public class JsonLReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => "jsonl";
    public string Category => "Reader Options";
    public Type OptionsType => typeof(JsonLReaderOptions);
    public bool CanHandle(string connectionString) => connectionString.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => false;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (JsonLReaderOptions)options;
        return new JsonLStreamReader(connectionString, opt, serviceProvider.GetRequiredService<ILogger<JsonLStreamReader>>());
    }
}
