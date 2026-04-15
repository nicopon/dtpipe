using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Xml;

public class XmlReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => XmlMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(XmlReaderOptions);
    public bool CanHandle(string connectionString) => XmlMetadata.CanHandle(connectionString);
    public bool SupportsStdio => XmlMetadata.SupportsStdio;
    public bool RequiresQuery => false;
    public bool YieldsColumnarOutput => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (XmlReaderOptions)options;
        return new XmlStreamReader(connectionString, opt, serviceProvider.GetRequiredService<ILogger<XmlStreamReader>>());
    }
}
