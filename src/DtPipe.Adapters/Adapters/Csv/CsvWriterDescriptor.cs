using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Csv;

public class CsvWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => "csv";
    public string Category => "Writer Options";
    public Type OptionsType => typeof(CsvWriterOptions);
    public bool CanHandle(string connectionString) => connectionString.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => false;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (CsvWriterOptions)options;
        return new CsvDataWriter(connectionString, opt);
    }
}
