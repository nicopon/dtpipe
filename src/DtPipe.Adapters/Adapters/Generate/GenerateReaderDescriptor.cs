using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Generate;

public class GenerateReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => GenerateMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(GenerateReaderOptions);
    public bool CanHandle(string connectionString) => GenerateMetadata.CanHandle(connectionString);
    public bool SupportsStdio => GenerateMetadata.SupportsStdio;
    public bool RequiresQuery => false;
    public bool YieldsColumnarOutput => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (GenerateReaderOptions)options;
        return new GenerateReader(connectionString, "", opt);
    }
}
