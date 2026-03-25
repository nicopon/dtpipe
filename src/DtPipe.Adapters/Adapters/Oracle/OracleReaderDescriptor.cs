using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Oracle;

public class OracleReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => OracleMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(OracleReaderOptions);
    public bool CanHandle(string connectionString) => OracleMetadata.CanHandle(connectionString);
    public bool SupportsStdio => OracleMetadata.SupportsStdio;
    public bool RequiresQuery => true;
    public bool YieldsColumnarOutput => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (OracleReaderOptions)options;
        return new OracleColumnarReader(connectionString, opt.Query ?? "SELECT 1", opt, 0);
    }
}
