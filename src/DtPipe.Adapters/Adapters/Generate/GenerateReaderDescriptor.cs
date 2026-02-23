using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Generate;

public class GenerateReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => "generate";
    public string Category => "Reader Options";
    public Type OptionsType => typeof(GenerateReaderOptions);
    public bool CanHandle(string connectionString) => connectionString.StartsWith("generate:", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => false;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (GenerateReaderOptions)options;
        return new GenerateReader(connectionString, "", opt);
    }
}
