using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.JsonL;

public class JsonLWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => "jsonl";
    public string Category => "Writer Options";
    public Type OptionsType => typeof(JsonLWriterOptions);
    public bool CanHandle(string connectionString) => connectionString.EndsWith(".jsonl", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => false;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (JsonLWriterOptions)options;
        return new JsonLDataWriter(connectionString, opt);
    }
}
