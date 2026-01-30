using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Configuration;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.DuckDB;

public class DuckDbWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ProviderName => DuckDbConstants.ProviderName;

    public Type OptionsType => typeof(DuckDbWriterOptions);

    public bool RequiresQuery => false;

    public bool CanHandle(string connectionString)
    {
        return DuckDbConnectionHelper.CanHandle(connectionString);
    }

    public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        var duckOptions = (DuckDbWriterOptions)options;
        
        // Ensure DataSource is present if it's just a file path
        if (!connectionString.Contains("DataSource=", StringComparison.OrdinalIgnoreCase) 
            && !connectionString.StartsWith("Data Source=", StringComparison.OrdinalIgnoreCase))
        {
            connectionString = $"Data Source={connectionString}";
        }

        return new DuckDbDataWriter(connectionString, duckOptions);
    }
}
