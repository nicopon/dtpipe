using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Oracle;

public class OracleReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ComponentName => "ora";
    public string Category => "Reader Options";
    public Type OptionsType => typeof(OracleReaderOptions);
    public bool CanHandle(string connectionString) => connectionString.StartsWith("oracle:", StringComparison.OrdinalIgnoreCase) || connectionString.Contains("User Id=", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => true;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (OracleReaderOptions)options;
        return new OracleStreamReader(connectionString, opt.Query ?? "SELECT 1", opt, 0);
    }
}
